package test

import (
	"fmt"
	"sync"
	"testing"
	"time"


	"github.com/stretchr/testify/assert"
	chk "github.com/vmware/vmware-go-kcl/clientlibrary/checkpoint"
	cfg "github.com/vmware/vmware-go-kcl/clientlibrary/config"
	wk "github.com/vmware/vmware-go-kcl/clientlibrary/worker"
	"github.com/vmware/vmware-go-kcl/logger"
)

const numShards = 4

func TestLeaseStealing(t *testing.T) {
	worker1, _, mService1 := createLeaseStealingTestWorker(1, t)
	worker2, _, mService2 := createLeaseStealingTestWorker(2, t)

	runLeaseStealingTest(worker1, mService1, worker2, mService2, numShards, t)
}

func TestLeaseStealingInjectCheckpointerAndLeasestealer(t *testing.T) {
	worker1, kclConfig1, mService1 := createLeaseStealingTestWorker(1, t)
	worker2, kclConfig2, mService2 := createLeaseStealingTestWorker(2, t)

	// Custom checkpointer and leasestealer
	checkpointer1 := chk.NewDynamoCheckpoint(kclConfig1)
	leasestealer1 := chk.NewDynamoLeasestealer(kclConfig1, checkpointer1)
	worker1.WithCheckpointer(checkpointer1).WithLeasestealer(leasestealer1)

	checkpointer2 := chk.NewDynamoCheckpoint(kclConfig2)
	leasestealer2 := chk.NewDynamoLeasestealer(kclConfig2, checkpointer2)
	worker2.WithCheckpointer(checkpointer2).WithLeasestealer(leasestealer2)

	runLeaseStealingTest(worker1, mService1, worker2, mService2, numShards, t)
}

func TestLeaseStealingWithMaxLeasesForWorker(t *testing.T) {
	maxLeasesForWorker := numShards - 1
	worker1, kclConfig1, mService1 := createLeaseStealingTestWorker(1, t)
	worker2, kclConfig2, mService2 := createLeaseStealingTestWorker(2, t)

	kclConfig1.WithMaxLeasesForWorker(maxLeasesForWorker)
	kclConfig2.WithMaxLeasesForWorker(maxLeasesForWorker)

	runLeaseStealingTest(worker1, mService1, worker2, mService2, maxLeasesForWorker, t)
}

func runLeaseStealingTest(worker1 *wk.Worker, monitoringService1 *leaseStealingMonitoringService, worker2 *wk.Worker, monitoringService2 *leaseStealingMonitoringService, initialLeases1 int, t *testing.T) {
	// Start worker 1
	err := worker1.Start()
	assert.Nil(t, err)

	// Publish records onto stream thoughtout the entire duration of the test
	done := make(chan int)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				// TODO not sure how to publish data in new pattern
				// err := worker1.Publish(streamName, uuid.New().String(), []byte(specstr))
				// if err != nil {
				// 	t.Errorf("Error in Publish. %+v", err)
				// }
				// time.Sleep(1 * time.Second)
			}
		}
	}()

	// Wait until the above worker has all leases
	var worker1ShardCount int
	for i := 0; i < 10; i++ {
		time.Sleep(5 * time.Second)
		monitoringService1.mux.Lock()
		worker1ShardCount = monitoringService1.shards
		monitoringService1.mux.Unlock()

		if worker1ShardCount == initialLeases1 {
			break
		}
	}

	// Assert correct number of leases
	assert.Equal(t, initialLeases1, worker1ShardCount, fmt.Sprintf("expected worker to have %d leases", initialLeases1))

	// start worker 2
	err = worker2.Start()
	assert.Nil(t, err)

	worker1ShardCount = 0
	var worker2ShardCount int
	// Wait until the both workers has 50% of the leases
	for i := 0; i < 10; i++ {
		time.Sleep(5 * time.Second)
		monitoringService1.mux.Lock()
		worker1ShardCount = monitoringService1.shards
		monitoringService1.mux.Unlock()

		monitoringService2.mux.Lock()
		worker2ShardCount = monitoringService2.shards
		monitoringService2.mux.Unlock()

		if worker1ShardCount == numShards/2 && worker2ShardCount == numShards/2 {
			break
		}
	}

	assert.Equal(t, numShards/2, worker1ShardCount)
	assert.Equal(t, numShards/2, worker2ShardCount)

	time.Sleep(10 * time.Second)
	worker1.Shutdown()
	worker2.Shutdown()

	// Close the done channel and wait for the goroutine to complete
	close(done)
	wg.Wait()
}

func createLeaseStealingTestWorker(id int, t *testing.T) (*wk.Worker, *cfg.KinesisClientLibConfiguration, *leaseStealingMonitoringService) {
	workerID := fmt.Sprintf("test-worker-%d", id)
	logConfig := logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Debug,
		ConsoleJSONFormat: false,
	}
	log := logger.NewLogrusLoggerWithConfig(logConfig)
	log.WithFields(logger.Fields{"worker": workerID})

	kclConfig := cfg.NewKinesisClientLibConfig(appName, streamName, regionName, workerID).
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(10000).
		WithLeaseStealing(true).WithLogger(log)

	// Configure Monitoring service
	mService := &leaseStealingMonitoringService{}
	kclConfig.WithMonitoringService(mService)

	// Create Worker-1
	worker := wk.NewWorker(recordProcessorFactory(t), kclConfig)
	return worker, kclConfig, mService
}

type leaseStealingMonitoringService struct {
	workerID string
	mux      *sync.Mutex
	shards   int
}

func (m *leaseStealingMonitoringService) Init(appName, streamName, workerID string) error {
	m.workerID = workerID
	m.mux = &sync.Mutex{}
	return nil
}

func (m *leaseStealingMonitoringService) Start() error { return nil }
func (m *leaseStealingMonitoringService) Shutdown()    {}

func (m *leaseStealingMonitoringService) IncrRecordsProcessed(shard string, count int)         {}
func (m *leaseStealingMonitoringService) IncrBytesProcessed(shard string, count int64)         {}
func (m *leaseStealingMonitoringService) MillisBehindLatest(shard string, millSeconds float64) {}
func (m *leaseStealingMonitoringService) LeaseGained(shard string) {
	m.mux.Lock()
	defer m.mux.Unlock()
	m.shards++
}

func (m *leaseStealingMonitoringService) LeaseLost(shard string) {
	m.mux.Lock()
	defer m.mux.Unlock()
	m.shards--
}

func (m *leaseStealingMonitoringService) LeaseRenewed(shard string)                           {}
func (m *leaseStealingMonitoringService) RecordGetRecordsTime(shard string, time float64)     {}
func (m *leaseStealingMonitoringService) RecordProcessRecordsTime(shard string, time float64) {}

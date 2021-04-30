/*
 * Copyright (c) 2018 VMware, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
// The implementation is derived from https://github.com/patrobinson/gokini
//
// Copyright 2018 Patrick robinson
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
package checkpoint

import (
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	cfg "github.com/vmware/vmware-go-kcl/clientlibrary/config"
	par "github.com/vmware/vmware-go-kcl/clientlibrary/partition"
)

func TestNewLeaseStealer(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true, item: map[string]*dynamodb.AttributeValue{}}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithLeaseStealing(true)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	err := checkpoint.Init()
	if err != nil {
		t.Errorf("Checkpoint initialization failed: %+v", err)
	}

	leasestealer := NewDynamoLeasestealer(kclConfig, checkpoint).WithDynamoDB(svc)
	err = leasestealer.Init()
	if err != nil {
		t.Errorf("Leasestealer initialization failed: %+v", err)
	}

	assert.Equal(t, kclConfig.Logger, leasestealer.log)
	assert.Equal(t, kclConfig.TableName, leasestealer.TableName)
	assert.Equal(t, kclConfig, leasestealer.kclConfig)
	assert.Equal(t, NumMaxRetries, leasestealer.Retries)
	assert.Equal(t, checkpoint, leasestealer.checkpointer)
}

func TestListActiveWorkers(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true, item: map[string]*dynamodb.AttributeValue{}}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithLeaseStealing(true)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	err := checkpoint.Init()
	if err != nil {
		t.Errorf("Checkpoint initialization failed: %+v", err)
	}

	leasestealer := NewDynamoLeasestealer(kclConfig, checkpoint).WithDynamoDB(svc)
	err = leasestealer.Init()
	if err != nil {
		t.Errorf("Leasestealer initialization failed: %+v", err)
	}

	shardStatus := map[string]*par.ShardStatus{
		"0000": {ID: "0000", AssignedTo: "worker_1", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0001": {ID: "0001", AssignedTo: "worker_2", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0002": {ID: "0002", AssignedTo: "worker_4", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0003": {ID: "0003", AssignedTo: "worker_0", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0004": {ID: "0004", AssignedTo: "worker_1", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0005": {ID: "0005", AssignedTo: "worker_3", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0006": {ID: "0006", AssignedTo: "worker_3", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0007": {ID: "0007", AssignedTo: "worker_0", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0008": {ID: "0008", AssignedTo: "worker_4", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0009": {ID: "0009", AssignedTo: "worker_2", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0010": {ID: "0010", AssignedTo: "worker_0", Checkpoint: ShardEnd, Mux: &sync.RWMutex{}},
	}

	workers, err := leasestealer.ListActiveWorkers(shardStatus)
	if err != nil {
		t.Error(err)
	}

	for workerID, shards := range workers {
		assert.Equal(t, 2, len(shards))
		for _, shard := range shards {
			assert.Equal(t, workerID, shard.AssignedTo)
		}
	}
}

func TestListActiveWorkersErrShardNotAssigned(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true, item: map[string]*dynamodb.AttributeValue{}}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithLeaseStealing(true)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	err := checkpoint.Init()
	if err != nil {
		t.Errorf("Checkpoint initialization failed: %+v", err)
	}

	leasestealer := NewDynamoLeasestealer(kclConfig, checkpoint).WithDynamoDB(svc)
	err = leasestealer.Init()
	if err != nil {
		t.Errorf("Leasestealer initialization failed: %+v", err)
	}

	shardStatus := map[string]*par.ShardStatus{
		"0000": {ID: "0000", Mux: &sync.RWMutex{}},
	}

	_, err = leasestealer.ListActiveWorkers(shardStatus)
	if err != ErrShardNotAssigned {
		t.Error("Expected ErrShardNotAssigned when shard is missing AssignedTo value")
	}
}

func TestClaimShard(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true, item: map[string]*dynamodb.AttributeValue{}}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithLeaseStealing(true)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	checkpoint.Init()

	leasestealer := NewDynamoLeasestealer(kclConfig, checkpoint).WithDynamoDB(svc)
	leasestealer.Init()

	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		"ShardID": {
			S: aws.String("0001"),
		},
		"AssignedTo": {
			S: aws.String("abcd-efgh"),
		},
		"LeaseTimeout": {
			S: aws.String(time.Now().AddDate(0, -1, 0).UTC().Format(time.RFC3339)),
		},
		"Checkpoint": {
			S: aws.String("deadbeef"),
		},
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String("TableName"),
		Item:      marshalledCheckpoint,
	}
	checkpoint.svc.PutItem(input)
	shard := &par.ShardStatus{
		ID:         "0001",
		Checkpoint: "deadbeef",
		Mux:        &sync.RWMutex{},
	}

	err := leasestealer.ClaimShard(shard, "ijkl-mnop")
	if err != nil {
		t.Errorf("Shard not claimed %s", err)
	}

	claimRequest, ok := svc.item[ClaimRequestKey]
	if !ok {
		t.Error("Expected claimRequest to be set by ClaimShard")
	} else if *claimRequest.S != "ijkl-mnop" {
		t.Errorf("Expected checkpoint to be ijkl-mnop. Got '%s'", *claimRequest.S)
	}

	status := &par.ShardStatus{
		ID:  shard.ID,
		Mux: &sync.RWMutex{},
	}
	checkpoint.FetchCheckpoint(status)

	// asiggnedTo, checkpointer, and parent shard id should be the same
	assert.Equal(t, shard.AssignedTo, status.AssignedTo)
	assert.Equal(t, shard.Checkpoint, status.Checkpoint)
	assert.Equal(t, shard.ParentShardId, status.ParentShardId)
}

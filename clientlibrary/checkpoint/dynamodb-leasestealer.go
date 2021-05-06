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
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"github.com/vmware/vmware-go-kcl/clientlibrary/config"
	par "github.com/vmware/vmware-go-kcl/clientlibrary/partition"
	"github.com/vmware/vmware-go-kcl/logger"
)

// DynamoLeasestealer implements the Leasestealer interface using DynamoDB as a backend
type DynamoLeasestealer struct {
	log          logger.Logger
	TableName    string
	svc          dynamodbiface.DynamoDBAPI
	kclConfig    *config.KinesisClientLibConfiguration
	Retries      int
	checkpointer Checkpointer
}

func NewDynamoLeasestealer(kclConfig *config.KinesisClientLibConfiguration, checkpointer Checkpointer) *DynamoLeasestealer {
	leasestealer := &DynamoLeasestealer{
		log:          kclConfig.Logger,
		TableName:    kclConfig.TableName,
		kclConfig:    kclConfig,
		Retries:      NumMaxRetries,
		checkpointer: checkpointer,
	}

	return leasestealer
}

// WithDynamoDB is used to provide DynamoDB service
func (leasestealer *DynamoLeasestealer) WithDynamoDB(svc dynamodbiface.DynamoDBAPI) *DynamoLeasestealer {
	leasestealer.svc = svc
	return leasestealer
}

// Init initialises the DynamoDB Leasestealer
func (leasestealer *DynamoLeasestealer) Init() error {
	leasestealer.log.Infof("Creating DynamoDB session")
	if leasestealer.svc == nil {
		s, err := session.NewSession(&aws.Config{
			Region:      aws.String(leasestealer.kclConfig.RegionName),
			Endpoint:    aws.String(leasestealer.kclConfig.DynamoDBEndpoint),
			Credentials: leasestealer.kclConfig.DynamoDBCredentials,
			Retryer:     client.DefaultRetryer{NumMaxRetries: leasestealer.Retries},
		})

		if err != nil {
			// no need to move forward
			leasestealer.log.Fatalf("Failed in getting DynamoDB session for creating Worker: %+v", err)
		}
		leasestealer.svc = dynamodb.New(s)
		return nil
	}

	return nil
}

// ListActiveWorkers returns a map of workers and their shards
func (leasestealer *DynamoLeasestealer) ListActiveWorkers(shardStatus map[string]*par.ShardStatus) (map[string][]*par.ShardStatus, error) {
	workers := map[string][]*par.ShardStatus{}
	for _, shard := range shardStatus {
		if shard.GetCheckpoint() == ShardEnd {
			continue
		}

		leaseOwner := shard.GetLeaseOwner()
		if leaseOwner == "" {
			leasestealer.log.Debugf("Shard Not Assigned Error. ShardID: %s, WorkerID: %s", shard.ID, leasestealer.kclConfig.WorkerID)
			return nil, ErrShardNotAssigned
		}
		if w, ok := workers[leaseOwner]; ok {
			workers[leaseOwner] = append(w, shard)
		} else {
			workers[leaseOwner] = []*par.ShardStatus{shard}
		}
	}
	return workers, nil
}

// ClaimShard places a claim request on a shard to signal a steal attempt
func (leasestealer *DynamoLeasestealer) ClaimShard(shard *par.ShardStatus, claimID string) error {
	err := leasestealer.checkpointer.FetchCheckpoint(shard)
	if err != nil && err != ErrSequenceIDNotFound {
		return err
	}
	leaseTimeoutString := shard.GetLeaseTimeout().Format(time.RFC3339)

	conditionalExpression := `ShardID = :id AND LeaseTimeout = :lease_timeout AND attribute_not_exists(ClaimRequest)`
	expressionAttributeValues := map[string]*dynamodb.AttributeValue{
		":id": {
			S: aws.String(shard.ID),
		},
		":lease_timeout": {
			S: aws.String(leaseTimeoutString),
		},
	}

	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		LeaseKeyKey: {
			S: &shard.ID,
		},
		LeaseTimeoutKey: {
			S: &leaseTimeoutString,
		},
		SequenceNumberKey: {
			S: &shard.Checkpoint,
		},
		ClaimRequestKey: {
			S: &claimID,
		},
	}

	if leaseOwner := shard.GetLeaseOwner(); leaseOwner == "" {
		conditionalExpression += " AND attribute_not_exists(AssignedTo)"
	} else {
		marshalledCheckpoint[LeaseOwnerKey] = &dynamodb.AttributeValue{S: &leaseOwner}
		conditionalExpression += "AND AssignedTo = :assigned_to"
		expressionAttributeValues[":assigned_to"] = &dynamodb.AttributeValue{S: &leaseOwner}
	}

	if checkpoint := shard.GetCheckpoint(); checkpoint == "" {
		conditionalExpression += " AND attribute_not_exists(Checkpoint)"
	} else if checkpoint == ShardEnd {
		conditionalExpression += " AND Checkpoint <> :checkpoint"
		expressionAttributeValues[":checkpoint"] = &dynamodb.AttributeValue{S: aws.String(ShardEnd)}
	} else {
		conditionalExpression += " AND Checkpoint = :checkpoint"
		expressionAttributeValues[":checkpoint"] = &dynamodb.AttributeValue{S: &checkpoint}
	}

	if shard.ParentShardId == "" {
		conditionalExpression += " AND attribute_not_exists(ParentShardId)"
	} else {
		marshalledCheckpoint[ParentShardIdKey] = &dynamodb.AttributeValue{S: aws.String(shard.ParentShardId)}
		conditionalExpression += " AND ParentShardId = :parent_shard"
		expressionAttributeValues[":parent_shard"] = &dynamodb.AttributeValue{S: &shard.ParentShardId}
	}

	return leasestealer.conditionalUpdate(conditionalExpression, expressionAttributeValues, marshalledCheckpoint)
}

func (leasestealer *DynamoLeasestealer) conditionalUpdate(conditionExpression string, expressionAttributeValues map[string]*dynamodb.AttributeValue, item map[string]*dynamodb.AttributeValue) error {
	return leasestealer.putItem(&dynamodb.PutItemInput{
		ConditionExpression:       aws.String(conditionExpression),
		TableName:                 aws.String(leasestealer.TableName),
		Item:                      item,
		ExpressionAttributeValues: expressionAttributeValues,
	})
}

func (leasestealer *DynamoLeasestealer) putItem(input *dynamodb.PutItemInput) error {
	_, err := leasestealer.svc.PutItem(input)
	return err
}

#!/bin/sh

aws dynamodb create-table \
    --table-name event_journal \
    --attribute-definitions \
        AttributeName=pid,AttributeType=S \
        AttributeName=seq_nr,AttributeType=N \
        AttributeName=entity_type_slice,AttributeType=S \
        AttributeName=ts,AttributeType=N \
    --key-schema \
        AttributeName=pid,KeyType=HASH \
        AttributeName=seq_nr,KeyType=RANGE \
    --provisioned-throughput \
        ReadCapacityUnits=5,WriteCapacityUnits=5 \
    --global-secondary-indexes \
            "[
                {
                    \"IndexName\": \"event_journal_slice_idx\",
                    \"KeySchema\": [
                        {\"AttributeName\":\"entity_type_slice\",\"KeyType\":\"HASH\"},
                        {\"AttributeName\":\"ts\",\"KeyType\":\"RANGE\"}
                    ],
                    \"Projection\": {
                        \"ProjectionType\":\"ALL\"
                    },
                    \"ProvisionedThroughput\": {
                        \"ReadCapacityUnits\": 5,
                        \"WriteCapacityUnits\": 5
                    }
                }
            ]"


aws dynamodb create-table \
    --table-name snapshot \
    --attribute-definitions \
        AttributeName=pid,AttributeType=S \
    --key-schema \
        AttributeName=pid,KeyType=HASH \
    --provisioned-throughput \
        ReadCapacityUnits=5,WriteCapacityUnits=5

aws dynamodb create-table \
    --table-name timestamp_offset \
    --attribute-definitions \
        AttributeName=name_slice,AttributeType=S \
        AttributeName=pid,AttributeType=S \
    --key-schema \
        AttributeName=name_slice,KeyType=HASH \
        AttributeName=pid,KeyType=RANGE \
    --provisioned-throughput \
        ReadCapacityUnits=5,WriteCapacityUnits=5

aws dynamodb create-table \
    --table-name projection_spec \
    --attribute-definitions \
        AttributeName=id,AttributeType=S \
    --key-schema \
        AttributeName=id,KeyType=HASH \
    --provisioned-throughput \
        ReadCapacityUnits=5,WriteCapacityUnits=5

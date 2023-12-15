# Simple producer and consumer clients using schema registry

This guide and provided code shows how to produce and consume data in Redpanda while making use of the schema registry. Clients are written in Go and make use of the franz-go library.

## Prerequisites

- docker (tested with 24.0.7)
- golang v1.19+ (tested with 1.21.5)

## Start Redpanda

Start Redpanda. Console is also included as it provides a simple way to view schemas that are in the registry:

```sh
docker compose -f compose.redpanda-0.yaml -f compose.console.yaml up
```

## Run clients

The producer client has a schema defined in code, and will first register this schema with the schema registry as the latest version under the subject "test-value". The client will then use this schema to serialize messages containing a timestamp. These messages will be sent to the "test" topic.

```sh
go run producer.go
```

> Note: If you already have a schema in the subject "test-value", then you must ensure any changes to the schema are compatible with the previous version. Otherwise (for testing) you can delete the previous schema version (1 in this case) with `curl -s -X DELETE "http://localhost:8081/subjects/test-value/versions/1"`.

In another terminal, start the consumer client. This will pull the latest schema from the registry, consume events from the "test" topic, and then deserialize with the schema and output the results.

```sh
go run consumer.go
```

## Cleanup

Once ready to cleanup, ctrl-c in any running terminals (for Redpanda and the clients). In the Redpanda terminal run the following command:

```sh
docker compose -f compose.redpanda-0.yaml -f compose.console.yaml down
```


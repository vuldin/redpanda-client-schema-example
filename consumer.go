// see https://github.com/twmb/franz-go/tree/master/examples/schema_registry

package main

import (
  "context"
  "flag"
  "fmt"
  "os"
  "strings"
  "time"

  "github.com/hamba/avro/v2"
  "github.com/twmb/franz-go/pkg/kgo"
  "github.com/twmb/franz-go/pkg/sr"
)

var (
  seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
  topic       = flag.String("topic", "test", "topic to produce to and consume from")
  registry    = flag.String("registry", "localhost:8081", "schema registry port to talk to")
)

func die(msg string, args ...any) {
  fmt.Fprintf(os.Stderr, msg+"\n", args...)
  os.Exit(1)
}

func maybeDie(err error, msg string, args ...any) {
  if err != nil {
    die(msg, args...)
  }
}

type example struct {
  A int64  `avro:"a"`
  B string `avro:"b"`
}

func main() {
  flag.Parse()

  // get the latest schema from the registry
  rcl, err := sr.NewClient(sr.URLs(*registry))
  maybeDie(err, "unable to create schema registry client: %v", err)
  ss, err := rcl.SchemaByVersion(context.Background(), *topic+"-value", -1)
  schemaText, err := rcl.SchemaTextByVersion(context.Background(), *topic+"-value", -1)
  fmt.Printf("using schema subject %q version %d id %d\n", ss.Subject, ss.Version, ss.ID)

  // configure deserialization
  avroSchema, err := avro.Parse(schemaText)
  maybeDie(err, "unable to parse avro schema: %v", err)
  var serde sr.Serde
  serde.Register(
    ss.ID,
    example{},
    sr.DecodeFn(func(b []byte, v any) error {
      return avro.Unmarshal(avroSchema, b, v)
    }),
  )

  // consume
  cl, err := kgo.NewClient(
    kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
    kgo.DefaultProduceTopic(*topic),
    kgo.ConsumeTopics(*topic),
  )
  maybeDie(err, "unable to init kgo client: %v", err)
  for {
    fs := cl.PollFetches(context.Background())
    fs.EachRecord(func(r *kgo.Record) {
      var ex example
      err := serde.Decode(r.Value, &ex)
      maybeDie(err, "unable to decode record value: %v")

      fmt.Printf("Consumed example: %+v, sleeping 1s\n", ex)
    })
    time.Sleep(time.Second)
  }
}


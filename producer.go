// see https://github.com/twmb/franz-go/tree/master/examples/schema_registry

package main

import (
  "context"
  "flag"
  "fmt"
  "os"
  "strconv"
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

var schemaText = `{
  "type": "record",
  "name": "simple",
  "namespace": "org.jlp.avro",
  "fields" : [
    {"name": "a", "type": "long"},
    {"name": "b", "type": "string"}
  ]
}`

type example struct {
  A int64  `avro:"a"`
  B string `avro:"b"`
}

func main() {
  flag.Parse()

  // register schema
  rcl, err := sr.NewClient(sr.URLs(*registry))
  maybeDie(err, "unable to create schema registry client: %v", err)
  ss, err := rcl.CreateSchema(context.Background(), *topic+"-value", sr.Schema{
    Schema: schemaText,
    Type:   sr.TypeAvro,
  })
  maybeDie(err, "unable to create avro schema: %v", err)
  fmt.Printf("created or reusing schema subject %q version %d id %d\n", ss.Subject, ss.Version, ss.ID)

  // configure serialization
  avroSchema, err := avro.Parse(schemaText)
  maybeDie(err, "unable to parse avro schema: %v", err)
  var serde sr.Serde
  serde.Register(
    ss.ID,
    example{},
    sr.EncodeFn(func(v any) ([]byte, error) {
      return avro.Marshal(avroSchema, v)
    }),
  )

  // produce
  cl, err := kgo.NewClient(
    kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
    kgo.DefaultProduceTopic(*topic),
    kgo.ConsumeTopics(*topic),
  )
  maybeDie(err, "unable to init kgo client: %v", err)
  for {
    var timeText = time.Now().Unix()
    cl.Produce(
      context.Background(),
      &kgo.Record{
        Value: serde.MustEncode(example{
          A: timeText,
          B: "hello",
        }),
      },
      func(r *kgo.Record, err error) {
        maybeDie(err, "unable to produce: %v", err)
        fmt.Printf("Produced avro-encoded record with timestamp: %s\n", strconv.FormatInt(timeText, 10))
      },
    )

    time.Sleep(time.Second)
  }
}


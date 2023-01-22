package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/kafka/adaptors/librd"
	"kafka-source-connector-etl/internal/pkg/uuid"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type rec struct {
	count       uint64
	lastUpdated time.Time
}

type mockHandler struct {
	topicCount int
	mm         sync.Map
	once       sync.Once
	printed    map[string]string
}

func (mockHandler) OnPartitionRevoked(ctx context.Context, session kafka.GroupSession) error {
	return nil
}
func (mockHandler) OnPartitionAssigned(ctx context.Context, session kafka.GroupSession) error {
	return nil
}
func (mockHandler) OnLost() error { return nil }
func (h *mockHandler) Consume(ctx context.Context, session kafka.GroupSession, partition kafka.PartitionClaim) error {
	h.once.Do(func() {
		go func() {
			for {
				h.mm.Range(func(key, value interface{}) bool {
					r := value.(rec)
					sec := time.Until(r.lastUpdated).Seconds()
					if math.Abs(sec) > 60 {
						_, ok := h.printed[key.(string)]
						if !ok {
							fmt.Printf("%v\t%v\n", key, r.count)
						}
						h.printed[key.(string)] = "0"
					}
					return true
				})
				if len(h.printed) == h.topicCount {
					fmt.Println("Done!")
					os.Exit(0)
					break
				}
			}
		}()
	})
	for rec := range partition.Records() {
		h.set(rec.Topic())
	}
	return nil
}

func (h *mockHandler) set(topic string) {
	i, ok := h.mm.Load(topic)
	if !ok {
		r := rec{
			lastUpdated: time.Now(),
		}
		atomic.AddUint64(&r.count, 1)
		h.mm.Store(topic, r)
		return
	}
	r := i.(rec)
	r.lastUpdated = time.Now()
	atomic.AddUint64(&r.count, 1)
	h.mm.Store(topic, r)
}

func initConsumer(broker string, topics []string) {
	cfg := librd.NewGroupConsumerConfig()
	cfg.BootstrapServers = []string{broker}
	cfg.GroupId = "tools." + uuid.New().String()
	cfg.Offsets.Initial = kafka.OffsetEarliest

	pc, err := librd.NewGroupConsumer(cfg)
	if err != nil {
		panic(err)
	}

	res := make(map[string]int)

	// get all service topics
	admin := librd.NewAdmin(cfg.BootstrapServers)

	tt, err := admin.ListTopics()
	if err != nil {
		panic(err)
	}

	if topics != nil || len(topics) != 0 {
		tt = topics
	}

	//tt = tt[0:2]
	fmt.Printf("found %v topics, starting..\n", len(tt))

	h := mockHandler{once: sync.Once{}, topicCount: len(tt), printed: map[string]string{}}
	err = pc.Subscribe(tt, &h)
	if err != nil {
		panic(err)
	}

	for tp, co := range res {
		fmt.Printf("%v %v\n", tp, co)
	}
}

func main() {
	broker := flag.String("bootstrap-servers", "localhost:9002", "--bootstrap-servers localhost:9092")
	ttStr := flag.String("topics", "mos.accounts,mos.clients", "--topics mos.accounts,mos.clients")
	flag.Parse()
	var topics []string
	if *ttStr != "" {
		tt := strings.Split(*ttStr, ",")
		topics = make([]string, 0)
		for _, t := range tt {
			tp := strings.ReplaceAll(t, " ", "")
			topics = append(topics, tp)
		}
	}
	initConsumer(*broker, topics)
}

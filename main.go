package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/kafka/adaptors/librd"
	"kafka-source-connector-etl/internal/pkg/uuid"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type rec struct {
	count       uint64
	lastUpdated time.Time
}

var mu sync.Mutex

type mockHandler struct {
	topicCount int
	mm         sync.Map
	once       sync.Once
	printed    map[string]string
}

func (mockHandler) OnPartitionRevoked(ctx context.Context, session kafka.GroupSession) error {
	//fmt.Println("OnPartitionRevoked")
	return nil
}
func (mockHandler) OnPartitionAssigned(ctx context.Context, session kafka.GroupSession) error {
	//fmt.Println("OnPartitionAssigned")
	return nil
}
func (mockHandler) OnLost() error { return nil }
func (h *mockHandler) Consume(ctx context.Context, session kafka.GroupSession, partition kafka.PartitionClaim) error {
	//h.once.Do(func() {
	//	go func() {
	//		for {
	//			h.mm.Range(func(key, value interface{}) bool {
	//				r := value.(rec)
	//				sec := time.Until(r.lastUpdated).Seconds()
	//				if math.Abs(sec) > 300 {
	//					mu.Lock()
	//					_, ok := h.printed[key.(string)]
	//					if !ok {
	//						fmt.Printf("%v\t%v\n", key, r.count)
	//					}
	//					h.printed[key.(string)] = "0"
	//					mu.Unlock()
	//				}
	//				return true
	//			})
	//			if len(h.printed) == h.topicCount {
	//				fmt.Println("Done!")
	//				os.Exit(0)
	//				break
	//			}
	//		}
	//	}()
	//})

	for rec := range partition.Records() {
		h.set(rec.Topic())
		err := session.MarkOffset(ctx, rec, "tools")
		if err != nil {
			fmt.Println(err)
		}

		err = session.CommitOffset(ctx, rec, "tools")
		if err != nil {
			fmt.Println(err)
		}
	}

	return nil
}

func (h mockHandler) printAll() {
	h.mm.Range(func(key, value interface{}) bool {
		r := value.(rec)
		fmt.Printf("%v\t%v\n", key, r.count)
		return false
	})
}

func (h *mockHandler) set(topic string) {
	i, ok := h.mm.Load(topic)
	if !ok {
		r := rec{
			count:       1,
			lastUpdated: time.Now(),
		}
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

	go func() {
		var stopChan = make(chan os.Signal, 2)
		signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

		<-stopChan // wait for SIGINT
		fmt.Println("results")
		h.printAll()
		os.Exit(0)
	}()

	pc, err := librd.NewGroupConsumer(cfg)
	if err != nil {
		panic(err)
	}
	err = pc.Subscribe(tt, &h)
	if err != nil {
		panic(err)
	}

}

func main() {
	broker := flag.String("bootstrap-servers", "localhost:9002", "--bootstrap-servers localhost:9092")
	ttStr := flag.String("topics", "", "--topics mos.accounts,mos.clients")
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

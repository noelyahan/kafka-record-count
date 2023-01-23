package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/kafka/adaptors/librd"
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
var mm sync.Map

type mockHandler struct {
	topicCount int
	mm         sync.Map
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
		return true
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

func printAll() {
	mm.Range(func(key, value interface{}) bool {
		r := value.(rec)
		fmt.Printf("%v\t%v\n", key, r.count)
		return true
	})
}

func set(topic string) {
	i, ok := mm.Load(topic)
	if !ok {
		r := rec{
			count:       1,
			lastUpdated: time.Now(),
		}
		mm.Store(topic, r)
		return
	}
	r := i.(rec)
	r.lastUpdated = time.Now()
	atomic.AddUint64(&r.count, 1)
	mm.Store(topic, r)
}

func initConsumer(broker string, topics []string) {
	// get all service topics
	admin := librd.NewAdmin([]string{broker})

	tt, err := admin.ListTopics()
	if err != nil {
		panic(err)
	}

	if topics != nil || len(topics) != 0 {
		tt = topics
	}

	fmt.Printf("found %v topics, starting..\n", len(tt))

	go func() {
		var stopChan = make(chan os.Signal, 2)
		signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

		<-stopChan // wait for SIGINT
		fmt.Println("results")
		printAll()
		os.Exit(0)
	}()

	wg := sync.WaitGroup{}
	for _, t := range tt {
		wg.Add(1)
		go func(topic string) {
			cfg := librd.NewConsumerConfig()
			cfg.BootstrapServers = []string{broker}
			pc, err := librd.NewPartitionConsumer(cfg)
			if err != nil {
				fmt.Println(err)
				return
			}
			mm, err := pc.ConsumeTopic(context.Background(), topic, kafka.OffsetEarliest)
			if err != nil {
				fmt.Println(err)
				return
			}
			for p, m := range mm {
				fmt.Println(fmt.Sprintf("topic: [%v], partition: [%v], start: [%v], end: [%v]",
					topic, p, m.BeginOffset(), m.EndOffset()))
				for range m.Events() {
					set(topic)
				}
			}
			fmt.Println("DONE>>>>>>>")
			wg.Done()
		}(t)
	}
	wg.Wait()
	fmt.Println("results")
	printAll()
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

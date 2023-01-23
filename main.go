package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/kafka/adaptors/librd"
	"math"
	"os"
	"os/signal"
	"strconv"
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
	res := ""
	mm.Range(func(key, value interface{}) bool {
		r := value.(rec)
		s := fmt.Sprintf("%v\t%v\n", key, r.count)
		res += s
		fmt.Print(s)
		return true
	})
	mode := int(0777)

	err := os.WriteFile("count.txt", []byte(res), os.FileMode(mode))
	if err != nil {
		panic(err)
	}
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
	bb := make([]string, 0)
	bb = append(bb, broker)
	if strings.Contains(broker, ",") {
		bb = make([]string, 0)
		s := strings.ReplaceAll(broker, " ", "")
		ss := strings.Split(s, ",")
		for _, b := range ss {
			bb = append(bb, b)
		}
	}

	admin := librd.NewAdmin(bb)

	tt, err := admin.ListTopics()
	if err != nil {
		panic(err)
	}

	tm, err := admin.FetchInfo(tt)
	if err != nil {
		panic(err)
	}

	mmt := make(map[string]int)
	if topics != nil || len(topics) != 0 {
		tt = topics
	}

	for _, t := range tt {
		mmt[t] = 0
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

	cfg := librd.NewConsumerConfig()
	cfg.BootstrapServers = bb
	pc, err := librd.NewPartitionConsumer(cfg)
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, t := range tm {
		fmt.Println(t.Name, t.NumPartitions)
		for i := 0; i < int(t.NumPartitions); i++ {
			wg.Add(1)
			done := false

			pp, err := pc.ConsumePartition(context.Background(), t.Name, int32(i), kafka.OffsetEarliest)
			if err != nil {
				panic(err)
			}

			fmt.Println(fmt.Sprintf("topic: [%v], partition: [%v], start: [%v], end: [%v]",
				t.Name, i, pp.BeginOffset(), pp.EndOffset()))

			diff := math.Abs(float64(pp.BeginOffset() - pp.EndOffset()))
			if diff == 0 {
				err := pp.Close()
				if err != nil {
					panic(err)
				}
				continue
			}
			for e := range pp.Events() {
				ss := strings.Split(e.String(), "@")
				//fmt.Println(ss)
				sOff := ss[len(ss)-1]
				off, err := strconv.Atoi(sOff)
				if err != nil {
					fmt.Println(err, e.String())
					err := pp.Close()
					if err != nil {
						panic(err)
					}
					done = true
					break
				}
				//fmt.Println(off)
				set(t.Name)
				if off+2 == int(pp.EndOffset()) {
					err := pp.Close()
					if err != nil {
						panic(err)
					}
					done = true
					break
				}
			}
			if done {
				fmt.Println(fmt.Sprintf("done [%v], partition: [%v]", t.Name, i))
				wg.Done()
				continue
			}
		}

	}

	//for _, t := range tt {
	//	wg.Add(1)
	//	go func(topic string) {
	//		mm, err := pc.ConsumeTopic(context.Background(), topic, kafka.OffsetEarliest)
	//		if err != nil {
	//			fmt.Println(err)
	//			return
	//		}
	//		for p, m := range mm {
	//
	//			done := false
	//			wg.Add(1)
	//			diff := math.Abs(float64(m.BeginOffset() - m.EndOffset()))
	//			// skip to next partition
	//			if diff == 0 {
	//				err := m.Close()
	//				if err != nil {
	//					panic(err)
	//				}
	//				wg.Done()
	//				continue
	//			}
	//
	//			fmt.Println(fmt.Sprintf("topic: [%v], total_p:[%v], partition: [%v], start: [%v], end: [%v]",
	//				topic, len(mm), p, m.BeginOffset(), m.EndOffset()))
	//			go func() {
	//				// partition wise events
	//				for e := range m.Events() {
	//					fmt.Println(e.String())
	//					ss := strings.Split(e.String(), "@")
	//					fmt.Println(ss)
	//					sOff := ss[len(ss)-1]
	//					off, err := strconv.Atoi(sOff)
	//					if err != nil {
	//						fmt.Println(err)
	//						err := m.Close()
	//						if err != nil {
	//							panic(err)
	//						}
	//						done = true
	//						break
	//					}
	//					//fmt.Println(e.String())
	//					set(topic)
	//
	//					if off+2 == int(m.EndOffset()) {
	//						err := m.Close()
	//						if err != nil {
	//							panic(err)
	//						}
	//						done = true
	//						break
	//					}
	//				}
	//
	//				if done {
	//					fmt.Println(fmt.Sprintf("done [%v]", t))
	//					wg.Done()
	//					return
	//				}
	//
	//			}()
	//		}
	//		wg.Done()
	//
	//	}(t)
	//}

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
	t := time.Now()
	initConsumer(*broker, topics)
	fmt.Println(fmt.Sprintf("total time mins: [%v]", time.Until(t).Minutes()))
}

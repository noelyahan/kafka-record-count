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

func printAll() {
	res := fmt.Sprintf("topic\ttotal-count\n\n")
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

func setCount(topic string) {
	mu.Lock()
	defer mu.Unlock()
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

	mmt := make(map[string]int)
	if topics != nil || len(topics) != 0 {
		tt = topics
	}

	for _, t := range tt {
		mmt[t] = 0
	}

	fmt.Printf("found %v topics, starting..\n", len(tt))

	cfg := librd.NewConsumerConfig()
	cfg.BootstrapServers = bb
	pc, err := librd.NewPartitionConsumer(cfg)
	if err != nil {
		fmt.Println(err)
		return
	}
	//tpc := "mos.banking.transactions.internal"
	//tpc := "mos.business.definitions"
	//tpc := "dlq-topic"

	wgTopic := sync.WaitGroup{}
	for _, topic := range tt {
		wgTopic.Add(1)
		go func(currentTopic string) {
			mp, err := pc.ConsumeTopic(context.Background(), currentTopic, kafka.OffsetEarliest)
			if err != nil {
				panic(err)
			}
			wgPartitions := sync.WaitGroup{}
			for i, tp := range mp {
				fmt.Println(fmt.Sprintf("consuming topic: [%v] partition: [%v]", currentTopic, i))
				wgPartitions.Add(1)
				go func(pp kafka.Partition) {
					for e := range pp.Events() {
						ctx, _ := context.WithTimeout(context.Background(), 4*time.Second)
						s := e.String()
						if !strings.Contains(s, "@") {
							continue
						}
						ss := strings.Split(s, "@")
						idx, err := strconv.Atoi(ss[len(ss)-1])
						if err != nil {
							panic(err)
						}

						setCount(currentTopic)

						diff := math.Abs(float64(idx) - float64(pp.EndOffset()))
						//fmt.Println("idx:", idx, "diff:", diff)

						if diff == 1 {
							//cb()
						}

						if diff == 2 {
							go func() {
								select {
								case <-ctx.Done():
									//fmt.Println("XXXXXXX")
									//fmt.Println("end.")
									if err := pp.Close(); err != nil {
										panic(err)
									}
									wgPartitions.Done()
									return
								}
							}()
						}

					}
				}(tp)
			}
			wgPartitions.Wait()
			wgTopic.Done()
			fmt.Println(fmt.Sprintf("completed topic: [%v]", currentTopic))
		}(topic)
	}

	wgTopic.Wait()
	printAll()
}

func v1(tt []string, bb []string) {
	admin := librd.NewAdmin(bb)
	tm, err := admin.FetchInfo(tt)
	if err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}

	cfg := librd.NewConsumerConfig()
	cfg.BootstrapServers = bb
	pc, err := librd.NewPartitionConsumer(cfg)
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, t := range tm {
		fmt.Println(fmt.Sprintf("topic: [%v], partitions: [%v]", t.Name, t.NumPartitions))
		for i := 0; i < int(t.NumPartitions); i++ {
			//pConsume(wg, pc, t, i)
			wg.Add(1)

			go func(x int, tp *kafka.Topic) {
				pConsume(sync.WaitGroup{}, pc, tp, x)
				wg.Done()
			}(i, t)
		}

	}

	wg.Wait()

	for {
		c := 0
		mm.Range(func(key, value interface{}) bool {
			c += 1
			return true
		})
		fmt.Printf("topic diff: [%v]\n", math.Abs(float64(len(tt)-c)))
		if c == len(tt) {
			break
		}
	}
}

func pConsume(wg sync.WaitGroup, pc kafka.PartitionConsumer, t *kafka.Topic, i int) bool {

	wg.Add(1)
	defer wg.Done()
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
		return true
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
			//done = true
			//break
			off = 0
		}
		//fmt.Println(off)
		setCount(t.Name)
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
		return done
	}

	return done

}

func main() {
	broker := flag.String("bootstrap-servers", "localhost:9092", "--bootstrap-servers localhost:9092")
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

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
			printAll()
		}
	}()

	go func() {
		var stopChan = make(chan os.Signal, 2)
		signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

		<-stopChan // wait for SIGINT
		printAll()
		os.Exit(0)
	}()

	t := time.Now()
	initConsumer(*broker, topics)
	fmt.Println(fmt.Sprintf("total time mins: [%v]", time.Until(t).Minutes()))
}

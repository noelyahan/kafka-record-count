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
	count uint64
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

func setCount(topic string, def ...int) {
	mu.Lock()
	defer mu.Unlock()
	i, ok := mm.Load(topic)
	if !ok {
		r := rec{
			count: 0,
		}
		mm.Store(topic, r)
		if len(def) != 0 {
			return
		}
	}
	i, _ = mm.Load(topic)
	r := i.(rec)
	atomic.AddUint64(&r.count, 1)
	mm.Store(topic, r)
}

func initRecCount(brokers []string, topics []string) {
	admin := librd.NewAdmin(brokers)

	tt, err := admin.ListTopics()
	if err != nil {
		panic(err)
	}

	// override with given topics
	if topics != nil || len(topics) != 0 {
		tt = topics
	}

	fmt.Printf("found %v topics, starting..\n", len(tt))

	cfg := librd.NewConsumerConfig()
	cfg.BootstrapServers = brokers
	pc, err := librd.NewPartitionConsumer(cfg)
	if err != nil {
		fmt.Println(err)
		return
	}

	wgTopic := sync.WaitGroup{}
	//testTopics := []string{"mos.clients", "dlq-topic", "_schemas"}
	for _, topic := range tt {
		wgTopic.Add(1)
		go func(currentTopic string) {
			mp, err := pc.ConsumeTopic(context.Background(), currentTopic, kafka.OffsetEarliest)
			if err != nil {
				panic(err)
			}
			fmt.Println(fmt.Sprintf("consuming topic: [%v]", currentTopic))
			wgPartitions := sync.WaitGroup{}
			for _, tp := range mp {
				//fmt.Println(fmt.Sprintf("consuming topic: [%v] partition: [%v]", currentTopic, i))
				wgPartitions.Add(1)
				go func(pp kafka.Partition) {
					for e := range pp.Events() {
						ctx, cb := context.WithTimeout(context.Background(), 4*time.Second)
						diff := math.Abs(float64(pp.BeginOffset() - pp.EndOffset()))
						if diff == 0 {
							setCount(currentTopic, 0)
							cb()
							go func() {
								select {
								case <-ctx.Done():
									if err := pp.Close(); err != nil {
										panic(err)
									}
									wgPartitions.Done()
									return
								}
							}()
						}
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

						diff = math.Abs(float64(idx) - float64(pp.EndOffset()))
						//fmt.Println("idx:", idx, "diff:", diff)

						if diff == 1 {
							//cb()
						}

						if diff == 2 {
							go func() {
								select {
								case <-ctx.Done():
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

func getArr(s string) []string {
	ss := strings.Split(s, ",")
	vv := make([]string, 0)
	for _, t := range ss {
		v := strings.ReplaceAll(t, " ", "")
		vv = append(vv, v)
	}
	return vv
}

func main() {
	broker := flag.String("bootstrap-servers", "localhost:9092", "--bootstrap-servers localhost:9092")
	ttStr := flag.String("topics", "", "--topics mos.accounts,mos.clients")
	flag.Parse()
	var topics []string
	if *ttStr != "" {
		topics = getArr(*ttStr)
	}

	brokers := make([]string, 0)
	brokers = append(brokers, *broker)
	if strings.Contains(*broker, ",") {
		brokers = getArr(*broker)
	}

	defer func() {
		if r := recover(); r != nil {
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
	initRecCount(brokers, topics)
	fmt.Println(fmt.Sprintf("total time mins: [%v]", math.Abs(time.Until(t).Minutes())))
}

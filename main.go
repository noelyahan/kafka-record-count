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
var cu map[string]int

func printAll() {
	res := fmt.Sprintf("topic\ttotal-count\n\n")
	for k, v := range cu {
		s := fmt.Sprintf("%v\t%v\n", k, v)
		res += s
		fmt.Print(s)
	}
	mode := int(0777)

	err := os.WriteFile("count.txt", []byte(res), os.FileMode(mode))
	if err != nil {
		panic(err)
	}
}

func setCount(topic string, c int) {
	mu.Lock()
	defer mu.Unlock()
	if cu == nil {
		cu = make(map[string]int)
	}

	_, ok := cu[topic]
	if !ok {
		cu[topic] = 0
	}

	v := cu[topic]
	v += c
	cu[topic] = v
}

func consumePartition(conf *librd.ConsumerConfig, wg *sync.WaitGroup, tp string, pt int32) {
	pc, err := librd.NewPartitionConsumer(conf)
	if err != nil {
		panic(err)
	}
	partition, err := pc.ConsumePartition(context.Background(), tp, pt, kafka.OffsetEarliest)
	if err != nil {
		panic(err)
	}

Lp:
	for ev := range partition.Events() {
		switch msg := ev.(type) {
		case kafka.Record:
			setCount(msg.Topic(), 1)
		case *kafka.PartitionEnd:
			wg.Done()
			break Lp
		}
	}
}

func conf(bb []string) *librd.ConsumerConfig {
	cfg := librd.NewConsumerConfig()
	cfg.BootstrapServers = bb
	return cfg
}

func initRecCount(brokers []string, topics []string, timeout time.Duration) {
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

	tpInfo, err := admin.FetchInfo(tt)
	if err != nil {
		panic(err)
	}

	wgTopic := sync.WaitGroup{}
	completedCount := int32(0)
	for _, topic := range tt {
		setCount(topic, 0)
		wgTopic.Add(1)
		go func(currentTopic string) {
			tpCfg := tpInfo[currentTopic]

			wgPartitions := sync.WaitGroup{}
			for i := 0; i < int(tpCfg.NumPartitions); i++ {
				wgPartitions.Add(1)
				go consumePartition(conf(brokers), &wgPartitions, currentTopic, int32(i))
			}
			wgPartitions.Wait()
			wgTopic.Done()
			atomic.AddInt32(&completedCount, 1)
			fmt.Println(fmt.Sprintf("completed topic [%v]: [%v]", completedCount, currentTopic))
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
	timeoutStr := flag.String("timeout", "120s", "--timeout 120s")
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

	timeout := time.Duration(120) * time.Second
	if *timeoutStr != "" {
		s := strings.ReplaceAll(*timeoutStr, "s", "")
		i, err := strconv.Atoi(s)
		if err != nil {
			panic(err)
		}
		timeout = time.Duration(i) * time.Second
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
	initRecCount(brokers, topics, timeout)
	fmt.Println(fmt.Sprintf("total time mins: [%v]", math.Abs(time.Until(t).Minutes())))
}

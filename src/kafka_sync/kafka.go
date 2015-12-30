package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"

	l4g "base/log4go"
)

const (
	STATE_OPEN  = 0
	STATE_CLOSE = 1
)

const (
	KAFKA_FLUSH_MSGS            = 1024
	KAFKA_FLUSH_FREQUENCY       = 50
	KAFKA_FETCH_DEFAULT         = 512
	KAFKA_CONSUMER_READ_TIMEOUT = 5
)

type Syncer interface {
	Process(<-chan struct{})
}

type SyncManager struct {
	CloseChan chan struct{}
	wg        sync.WaitGroup
	state     int32
}

func NewSyncManager() *SyncManager {
	return &SyncManager{
		CloseChan: make(chan struct{}),
	}
}

func (this *SyncManager) Add(delta int) {
	this.wg.Add(delta)
}

func (this *SyncManager) Create(se Syncer) {
	go func() {
		defer func() {
			this.wg.Done()
			this.Close()
			if se != nil {
				l4g.Info("syncer close: %s %d", se.(*KafkaSync).topic, se.(*KafkaSync).partition)
			}
		}()
		if se != nil {
			se.Process(this.CloseChan)
		}
	}()
}

func (this *SyncManager) Close() {
	if atomic.CompareAndSwapInt32(&this.state, STATE_OPEN, STATE_CLOSE) {
		close(this.CloseChan)
	}
}

func (this *SyncManager) Wait() {
	this.wg.Wait()
}

type KafkaSync struct {
	ldb          *LevelDB
	consumerList string
	producerList string
	topic        string
	partition    int32
}

func NewKafkaSync(ldb *LevelDB, srcList, dstList, topic string, partition int32) *KafkaSync {
	ret := &KafkaSync{
		ldb:          ldb,
		consumerList: srcList,
		producerList: dstList,
		topic:        topic,
		partition:    partition,
	}
	return ret
}

func (this *KafkaSync) Process(cc <-chan struct{}) {
	// init leveldb key
	leveldbKey := []byte(fmt.Sprintf("%s_%d", this.topic, this.partition))

	//init producer
	producer_config := sarama.NewConfig()
	producer_config.Producer.Flush.Messages = KAFKA_FLUSH_MSGS
	producer_config.Producer.Flush.Frequency = KAFKA_FLUSH_FREQUENCY * time.Millisecond
	producer_config.Producer.Return.Successes = true
	producer, err := sarama.NewAsyncProducer(strings.Split(this.producerList, ","), producer_config)
	if err != nil {
		l4g.Error("new producer %s %d error: %s", this.producerList, this.partition, err.Error())
		return
	}

	defer func() {
		producer.AsyncClose()
		for msg := range producer.Successes() {
			if err := this.ldb.Put(leveldbKey, []byte(strconv.FormatInt(msg.Metadata.(int64), 10))); err != nil {
				l4g.Error("leveldb put (%s %d %d) error: %s", this.topic, this.partition, msg.Metadata.(int64), err.Error())
			}
		}
		l4g.Info("producer close: %s %d", this.topic, this.partition)
	}()

	//init consumer
	consumer_config := sarama.NewConfig()
	consumer_config.Net.ReadTimeout = KAFKA_CONSUMER_READ_TIMEOUT * time.Minute
	consumer_config.Consumer.Fetch.Default = KAFKA_FETCH_DEFAULT
	consumer_config.Consumer.MaxProcessingTime = time.Second
	consumer_config.Consumer.Return.Errors = true
	consumerClient, cErr := sarama.NewClient(strings.Split(this.consumerList, ","), consumer_config)
	if cErr != nil {
		l4g.Error("new client error: %s %d %s %s", this.topic, this.partition, this.consumerList, cErr.Error())
		return
	}

	defer func() {
		if err := consumerClient.Close(); err != nil {
			l4g.Error("close client error: %s %d %s", this.topic, this.partition, err.Error())
		}
		l4g.Info("consumer client close: %s %d", this.topic, this.partition)
	}()

	consumer, err := sarama.NewConsumerFromClient(consumerClient)
	if err != nil {
		l4g.Error("new consumer %s %d error: %s", this.topic, this.partition, err.Error())
		return
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			l4g.Error("close consumer (%s %d) error: %s", this.topic, this.partition, err.Error())
		}
		l4g.Info("consumer close: %s %d", this.topic, this.partition)
	}()

	//get right consume offset
	ret, err := this.ldb.Get(leveldbKey)
	if err != nil {
		l4g.Error("leveldb get (%s %d) error: %s", this.topic, this.partition, err.Error())
		return
	}

	var offset int64
	if len(ret) > 0 {
		offset, _ = strconv.ParseInt(string(ret), 10, 64)
	} else {
		offset = sarama.OffsetNewest
	}

	l4g.Info("consume offset: %s %d %d", this.topic, this.partition, offset)

	//init partition consumer
	partitionConsumer, err := consumer.ConsumePartition(this.topic, this.partition, offset)
	if err != nil {
		if kerr, ok := err.(sarama.KError); ok && kerr == sarama.ErrOffsetOutOfRange {
			oldestOffset, _ := consumerClient.GetOffset(this.topic, this.partition, sarama.OffsetOldest)
			newestOffset, _ := consumerClient.GetOffset(this.topic, this.partition, sarama.OffsetNewest)
			l4g.Error("offset out of range: %s %d %d %d %d", this.topic, this.partition, offset, oldestOffset, newestOffset)
			partitionConsumer, err = consumer.ConsumePartition(this.topic, this.partition, sarama.OffsetNewest)
			if err != nil {
				l4g.Error("renew consume partition (%s %d) error: %s", this.topic, this.partition, err.Error())
				return
			}
		} else {
			l4g.Error("new consume partition (%s %d) error: %s", this.topic, this.partition, err.Error())
			return
		}
	}

	defer func() {
		if errs := partitionConsumer.Close(); errs != nil {
			for _, err := range errs.(sarama.ConsumerErrors) {
				l4g.Error("close partition consumer(%s %d) error: %s", this.topic, this.partition, err.Error())
			}
		}
		l4g.Info("partition consumer close: %s %d", this.topic, this.partition)
	}()

	ticker := time.NewTicker(time.Minute)
	lastOffset, newOffset := offset, offset

	var producerChan chan<- *sarama.ProducerMessage
	var producerMsg *sarama.ProducerMessage

	for {
		select {
		case <-cc:
			l4g.Info("manager close %s %d", this.topic, this.partition)
			return
		case msg := <-partitionConsumer.Messages():
			producerMsg = &sarama.ProducerMessage{
				Topic:    msg.Topic,
				Key:      sarama.ByteEncoder(msg.Key),
				Value:    sarama.ByteEncoder(msg.Value),
				Metadata: msg.Offset,
			}
			producerChan = producer.Input()
		case producerChan <- producerMsg:
			producerChan = nil
		case err := <-partitionConsumer.Errors():
			if kerr, ok := err.Err.(sarama.KError); ok && kerr == sarama.ErrOffsetOutOfRange {
				oldestOffset, _ := consumerClient.GetOffset(this.topic, this.partition, sarama.OffsetOldest)
				l4g.Error("partition consumer (%s %d) return error: %s %d %d", this.topic, this.partition, err.Error(), newOffset, oldestOffset)
			} else {
				l4g.Error("partition consumer (%s %d) return error: %s", this.topic, this.partition, err.Error())
			}
			return
		case msg := <-producer.Successes():
			newOffset = msg.Metadata.(int64)
			if err := this.ldb.Put(leveldbKey, []byte(strconv.FormatInt(newOffset, 10))); err != nil {
				l4g.Error("leveldb put (%s %d %d) error: %s", this.topic, this.partition, newOffset, err.Error())
				return
			}
		case err := <-producer.Errors():
			l4g.Error("producer (%s %d) return error: %s", this.topic, this.partition, err.Error())
			return
		case <-ticker.C:
			l4g.Info("consume offset: %s %d %d %d", this.topic, this.partition, newOffset, newOffset-lastOffset)
			lastOffset = newOffset
		}
	}
}

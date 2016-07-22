package main

import (
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

type Syncer interface {
	GetLevelDBKey() []byte
	Process(<-chan struct{})
	Close()
}

type SyncFn func(*SourceKafka, *xmlConfig) Syncer

type SyncManager struct {
	CloseChan chan struct{}
	wg        sync.WaitGroup
	state     int32
	FnM       map[string]SyncFn
}

func NewSyncManager() *SyncManager {
	return &SyncManager{
		CloseChan: make(chan struct{}),
		FnM:       make(map[string]SyncFn),
	}
}

func (this *SyncManager) AddWaitGroup(delta int) {
	this.wg.Add(delta)
}

func (this *SyncManager) Wait() {
	this.wg.Wait()
}

func (this *SyncManager) Close() {
	if atomic.CompareAndSwapInt32(&this.state, STATE_OPEN, STATE_CLOSE) {
		close(this.CloseChan)
	}
}

func (this *SyncManager) Create(cfg *xmlConfig, index int32, offset int64) {
	go func() {
		defer func() {
			this.wg.Done()
			this.Close()
		}()
		skafka := &SourceKafka{
			config:         &cfg.Source,
			PartitionIndex: index,
		}
		syn := this.CreateSync(skafka, cfg)
		if syn != nil {
			skafka.Init(syn.GetLevelDBKey(), true, offset)
			syn.Process(this.CloseChan)
			defer syn.Close()
		}
		defer skafka.Close()
	}()
}

func (this *SyncManager) CreateSync(sk *SourceKafka, cfg *xmlConfig) Syncer {
	return this.FnM[cfg.GetDestinationName()](sk, cfg)
}

func (this *SyncManager) RegisterSync(dst string, fn SyncFn) {
	this.FnM[dst] = fn
}

type SourceKafka struct {
	config *xmlSource

	ConsumerClient    sarama.Client
	Consumer          sarama.Consumer
	PartitionConsumer sarama.PartitionConsumer
	PartitionIndex    int32
}

func (this *SourceKafka) Topic() string {
	return this.config.Topic.Id
}

func (this *SourceKafka) Init(leveldbKey []byte, initConsumer bool, configOffset int64) bool {
	topic := this.Topic()
	consumer_config := sarama.NewConfig()
	consumer_config.Consumer.Fetch.Default = this.config.ConsumerFetchSize
	consumer_config.Consumer.Fetch.Min = this.config.ConsumerFetchMinSize
	consumer_config.Net.ReadTimeout = time.Duration(this.config.ConsumerNetReadTimeout) * time.Minute
	consumer_config.Consumer.MaxProcessingTime = time.Second
	consumer_config.Consumer.Return.Errors = true
	consumerClient, cErr := sarama.NewClient(strings.Split(this.config.Brokers, ","), consumer_config)
	if cErr != nil {
		l4g.Error("new client error: %s %d %s %s", topic, this.PartitionIndex, this.config.Brokers, cErr.Error())
		return false
	}
	this.ConsumerClient = consumerClient

	if !initConsumer {
		return true
	}

	consumer, err := sarama.NewConsumerFromClient(consumerClient)
	if err != nil {
		l4g.Error("new consumer %s %d error: %s", topic, this.PartitionIndex, err.Error())
		return false
	}
	this.Consumer = consumer

	//get right consume offset
	offset, err := this.GetLevelDBOffset(leveldbKey)
	if err != nil {
		l4g.Error("get leveldb key error: %s %s", leveldbKey, err.Error())
		return false
	}
	if offset < 0 {
		offset = configOffset
	} else {
		if offset < configOffset {
			offset = configOffset
		}
		offset++
	}
	//l4g.Info("consume offset: %s %d %d", topic, this.PartitionIndex, offset)
	l, n := this.GetKafkaOffset()
	l4g.Info("consume offset: %s %d %d %d %d", topic, this.PartitionIndex, offset, l, n)

	//init partition consumer
	partitionConsumer, err := consumer.ConsumePartition(topic, this.PartitionIndex, offset)
	if err != nil {
		l4g.Error("new consume partition (%s %d) error: %s", topic, this.PartitionIndex, err.Error())
		return false
	}
	this.PartitionConsumer = partitionConsumer
	return true
}

func (this *SourceKafka) Close() {
	if this.PartitionConsumer != nil {
		if errs := this.PartitionConsumer.Close(); errs != nil {
			for _, err := range errs.(sarama.ConsumerErrors) {
				l4g.Error("close partition consumer(%s %d) error: %s", this.Topic(), this.PartitionIndex, err.Error())
			}
		}
		l4g.Info("partition consumer close: %s %d", this.Topic(), this.PartitionIndex)
	}

	if this.Consumer != nil {
		if err := this.Consumer.Close(); err != nil {
			l4g.Error("close consumer (%s %d) error: %s", this.Topic(), this.PartitionIndex, err.Error())
		}
		l4g.Info("consumer close: %s %d", this.Topic(), this.PartitionIndex)
	}

	if this.ConsumerClient != nil {
		if err := this.ConsumerClient.Close(); err != nil {
			l4g.Error("close client error: %s %d %s", this.Topic(), this.PartitionIndex, err.Error())
			l4g.Info("consumer client close: %s %d", this.Topic(), this.PartitionIndex)
		}
	}
}

func (this *SourceKafka) GetKafkaOffset() (int64, int64) {
	oldestOffset, _ := this.ConsumerClient.GetOffset(this.Topic(), this.PartitionIndex, sarama.OffsetOldest)
	newestOffset, _ := this.ConsumerClient.GetOffset(this.Topic(), this.PartitionIndex, sarama.OffsetNewest)
	return oldestOffset, newestOffset
}

func (this *SourceKafka) GetLevelDBOffset(key []byte) (int64, error) {
	ret, err := gldb.Get(key)
	if err != nil {
		return 0, err
	}

	var offset int64
	if len(ret) > 0 {
		offset, err = strconv.ParseInt(string(ret), 10, 64)
	} else {
		offset = sarama.OffsetOldest
	}
	return offset, err
}

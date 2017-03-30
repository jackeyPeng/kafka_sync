package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"

	l4g "base/log4go"
)

type SyncKafka struct {
	src        *SourceKafka
	leveldbKey []byte

	config   *xmlDestinationKafka
	producer sarama.AsyncProducer
}

func NewSyncKafka(src *SourceKafka, config *xmlConfig) Syncer {
	ret := &SyncKafka{
		src:    src,
		config: &config.Destination.Kafka,
	}
	ret.leveldbKey = []byte(fmt.Sprintf("%s_%s_%d", ret.config.Name, ret.config.Topic, ret.src.PartitionIndex))
	return ret
}

func (this *SyncKafka) GetLevelDBKey() []byte {
	return this.leveldbKey
}

func (this *SyncKafka) Process(cc <-chan struct{}) {
	//init producer
	producer_config := sarama.NewConfig()
	producer_config.Producer.Flush.Messages = this.config.ProducerFlushSize
	producer_config.Producer.Flush.Frequency = time.Duration(this.config.ProducerFlushFrequency) * time.Millisecond
	producer_config.Producer.Return.Successes = true
	producer, err := sarama.NewAsyncProducer(strings.Split(this.config.Brokers, ","), producer_config)
	if err != nil {
		l4g.Error("new producer %s %d error: %s", this.config.Brokers, this.src.PartitionIndex, err.Error())
		return
	}
	this.producer = producer

	ticker := time.NewTicker(time.Minute)
	offset, _ := this.src.GetLevelDBOffset(this.leveldbKey)
	lastOffset, newOffset := offset, offset
	consumerMsgs := this.src.PartitionConsumer.Messages()
	var producerMsgs chan<- *sarama.ProducerMessage
	var producerMsg *sarama.ProducerMessage

	l4g.Info("%s %d start", this.config.Topic, this.src.PartitionIndex)

	for {
		select {
		case <-cc:
			l4g.Info("manager kafka close %s %d", this.config.Topic, this.src.PartitionIndex)
			return
		case <-ticker.C:
			l4g.Info("consume offset: %s %d %d %d", this.src.Topic(), this.src.PartitionIndex, newOffset, newOffset-lastOffset)
			lastOffset = newOffset
		case err := <-this.src.PartitionConsumer.Errors():
			if kerr, ok := err.Err.(sarama.KError); ok && kerr == sarama.ErrOffsetOutOfRange {
				oldestOffset, _ := this.src.GetKafkaOffset()
				l4g.Error("partition consumer (%s %d) return error: %s %d %d", this.src.Topic(), this.src.PartitionIndex, err.Error(), newOffset, oldestOffset)
			} else {
				l4g.Error("partition consumer (%s %d) return error: %s", this.src.Topic(), this.src.PartitionIndex, err.Error())
			}
			return
		case msg := <-consumerMsgs:
			consumerMsgs = nil
			producerMsg = &sarama.ProducerMessage{
				Topic:    this.config.Topic,
				Key:      sarama.ByteEncoder(msg.Key),
				Value:    sarama.ByteEncoder(msg.Value),
				Metadata: msg.Offset,
			}
			producerMsgs = producer.Input()
		case producerMsgs <- producerMsg:
			l4g.Debug("read %s %d offset: %d", this.src.Topic(), this.src.PartitionIndex, producerMsg.Metadata.(int64))
			consumerMsgs = this.src.PartitionConsumer.Messages()
			producerMsgs = nil
			producerMsg = nil
		case msg := <-producer.Successes():
			newOffset = msg.Metadata.(int64)
			if err := gldb.Put(this.leveldbKey, []byte(strconv.FormatInt(newOffset, 10))); err != nil {
				l4g.Error("leveldb put %s %d error: %s", this.leveldbKey, newOffset, err.Error())
				return
			}
			l4g.Debug("write %s %d offset: %d", this.leveldbKey, newOffset)
		case err := <-producer.Errors():
			l4g.Error("producer (%s %d) return error: %s", this.config.Topic, this.src.PartitionIndex, err.Error())
			return
		}
	}
}

func (this *SyncKafka) Close() {
	if this.producer != nil {
		this.producer.AsyncClose()
    for more := true; more; {
      select {
      case msg := <- this.producer.Successes():
        if err := gldb.Put(this.leveldbKey, []byte(strconv.FormatInt(msg.Metadata.(int64), 10))); err != nil {
          l4g.Error("leveldb put (%s %d %d) error: %s", this.config.Topic, this.src.PartitionIndex, msg.Metadata.(int64), err.Error())
        }
        l4g.Debug("write %s %d offset: %d", this.config.Topic, this.src.PartitionIndex, msg.Metadata.(int64))
      default:
        more = false
      }
    }
		l4g.Info("producer close: %s %d", this.config.Topic, this.src.PartitionIndex)
	}
}

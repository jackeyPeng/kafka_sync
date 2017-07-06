package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	l4g "base/log4go"

	"github.com/Shopify/sarama"
	"github.com/ivanabc/radix/redis"
)

type SyncRedis struct {
	src        *SourceKafka
	leveldbKey []byte

	config *xmlDestinationRedis
}

func NewSyncRedis(src *SourceKafka, config *xmlConfig) Syncer {
	ret := &SyncRedis{
		src:    src,
		config: &config.Destination.Redis,
	}
	ret.leveldbKey = []byte(fmt.Sprintf("%s_%d", ret.config.Name, ret.src.PartitionIndex))
	return ret
}

func (this *SyncRedis) GetLevelDBKey() []byte {
	return this.leveldbKey
}

func (this *SyncRedis) Process(cc <-chan struct{}) {
	client, err := redis.DialTimeout("tcp", this.config.NetAddr, time.Duration(this.config.TimeOut)*time.Second)
	if err != nil {
		l4g.Error("connect redis(%s) error: %s", this.config.NetAddr, err.Error())
		return
	}

	offset, _ := this.src.GetLevelDBOffset(this.leveldbKey)
	l4g.Debug("sync redis leveldb offset: %s %s %d", this.config.NetAddr, this.leveldbKey, offset)
	ticker := time.NewTicker(10 * time.Second)
	var lastPutCount, putCount uint64
	var count int
	var writeRate int
	var tickerCount int

	for {
		select {
		case <-cc:
			l4g.Info("manager redis close %d", this.src.PartitionIndex)
			return
		case err := <-this.src.PartitionConsumer.Errors():
			if kerr, ok := err.Err.(sarama.KError); ok && kerr == sarama.ErrOffsetOutOfRange {
				l4g.Error("partition consumer (%s %d) return error: %s", this.src.Topic(), this.src.PartitionIndex, err.Error())
			}
			return
		case msg := <-this.src.PartitionConsumer.Messages():
			value := string(msg.Value)
			name, key := this.GetRedisNameAndKey(value)
			if name != "" {
				r := client.Cmd("hmset", name, key, value)
				if r.Err != nil {
					l4g.Error("redis hmset op error: %s", r.Err.Error())
					return
				}
				offset = msg.Offset
				writeRate++
				count++
				putCount++
				if count == this.config.MaxMergeMsg {
					if err := gldb.Put(this.leveldbKey, []byte(strconv.FormatInt(offset, 10))); err != nil {
						l4g.Error("leveldb put %s %d error: %s", this.leveldbKey, offset, err.Error())
						return
					}
					count = 0
				} else if writeRate == this.config.WriteRate {
					if err := gldb.Put(this.leveldbKey, []byte(strconv.FormatInt(offset, 10))); err != nil {
						l4g.Error("leveldb put %s %d error: %s", this.leveldbKey, offset, err.Error())
						return
					}
					writeRate = 0
				}
			}
		case <-ticker.C:
			tickerCount++
			if tickerCount == 6 {
				tickerCount = 0
				l4g.Info("consume offset: %s %d %d", this.src.Topic(), this.src.PartitionIndex, putCount-lastPutCount)
				lastPutCount = putCount
			}

			if err := gldb.Put(this.leveldbKey, []byte(strconv.FormatInt(offset, 10))); err != nil {
				l4g.Error("leveldb put %s %d error: %s", this.leveldbKey, offset, err.Error())
				return
			}
		}
	}
}

func (this *SyncRedis) Close() {}

func (this *SyncRedis) GetRedisNameAndKey(value string) (name string, key string) {
	ss := strings.Split(value, ",")
	if len(ss) == 28 {
		name = ss[0] + ":" + ss[3]
		key = ss[27]
	} else {
		l4g.Error("message split len error: %s", value)
	}
	l4g.Debug("redis name and key: %s %s %s", name, key, value)
	return
}

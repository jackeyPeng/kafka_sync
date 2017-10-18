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

type splitLogFn func(string) ([]string, []string, []string)

func GetRedisNameAndKey(value string) (names []string, keys []string, values []string) {
	ss := strings.Split(value, ",")
	if len(ss) == 28 {
		name := ss[0] + ":" + ss[3]
		key := ss[27]
		names = []string{name}
		keys = []string{key}
		values = []string{value}
		l4g.Debug("redis name and key and value: %s %s %s", name, key, value)
	} else {
		l4g.Error("message split len error: %s", value)
	}
	return
}

func GetRedisNameAndKeyFromSXLog(value string) (names []string, keys []string, values []string) {
	ss := strings.Split(value, "|")
	strLen := len(ss)

	const table string = "w"
	const subtitleTable string = "ws"
	const chatTable string = "wc"

	if strLen >= 50 {
		if ss[2] == "0" {
			return
		}
		logTime := ss[strLen-1]
		names, keys, values = make([]string, 0, 2), make([]string, 0, 2), make([]string, 0, 2)
		if ss[4] == "4" && ss[5] == "19" {
			//行为日志
			names = append(names, fmt.Sprintf("%s:%s:%s", table, ss[0], ss[2]))
			keys = append(keys, logTime)
			ss[48] = ""
			values = append(values, strings.Join(ss, "|"))
			l4g.Debug("[sx] redis name and key and value: %s %s %s", names[0], keys[0], values[0])
			//弹幕日志
			names = append(names, fmt.Sprintf("%s:%s", subtitleTable, ss[0]))
			keys = append(keys, logTime)
			values = append(values, value)
			l4g.Debug("[sx] redis 1 name and key and value: %s %s %s", names[1], keys[1], values[1])
		} else if ss[4] == "4" && ss[5] == "116" {
			//行为日志
			names = append(names, fmt.Sprintf("%s:%s:%s", table, ss[0], ss[2]))
			keys = append(keys, logTime)
			ss[48] = ""
			values = append(values, strings.Join(ss, "|"))
			l4g.Debug("[sx] redis name and key and value: %s %s %s", names[0], keys[0], values[0])
			//聊天日志
			names = append(names, fmt.Sprintf("%s:%s", chatTable, ss[0]))
			keys = append(keys, logTime)
			values = append(values, value)
			l4g.Debug("[sx] redis 1 name and key and value: %s %s %s", names[1], keys[1], values[1])
		} else {
			names = append(names, fmt.Sprintf("%s:%s:%s", table, ss[0], ss[2]))
			keys = append(keys, logTime)
			values = append(values, value)
			l4g.Debug("[sx] redis name and key and value: %s %s %s", names[0], keys[0], values[0])
		}
	} else {
		l4g.Error("[sx] message split len error: %s", value)
	}
	return
}

type SyncRedis struct {
	src        *SourceKafka
	leveldbKey []byte

	config  *xmlDestinationRedis
	splitFn splitLogFn

	client      *redis.Client
	appendCount int
}

func NewSyncRedis(src *SourceKafka, config *xmlConfig) Syncer {
	ret := &SyncRedis{
		src:    src,
		config: &config.Destination.Redis,
	}
	if ret.config.LogType != "sx" {
		ret.splitFn = GetRedisNameAndKey
	} else {
		ret.splitFn = GetRedisNameAndKeyFromSXLog
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
	this.client = client
	defer this.client.Close()

	offset, _ := this.src.GetLevelDBOffset(this.leveldbKey)
	l4g.Debug("sync redis leveldb offset: %s %s %d", this.config.NetAddr, this.leveldbKey, offset)
	ticker := time.NewTicker(10 * time.Second)
	var lastPutCount, putCount uint64
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
			offset = msg.Offset
			value := string(msg.Value)
			names, keys, values := this.splitFn(value)
			for index := range names {
				this.client.Append("hmset", names[index], keys[index], values[index])
				this.appendCount++
				writeRate++
				putCount++
			}

			if this.appendCount >= this.config.MaxMergeMsg {
				if !this.finishRedisReply(offset) {
					return
				}
			} else if writeRate >= this.config.WriteRate {
				if !this.finishRedisReply(offset) {
					return
				}
				writeRate = 0
			}
		case <-ticker.C:
			tickerCount++
			if tickerCount == 6 {
				l4g.Info("consume offset: %s %d %d", this.src.Topic(), this.src.PartitionIndex, putCount-lastPutCount)
				tickerCount = 0
				lastPutCount = putCount
			}
			if !this.finishRedisReply(offset) {
				return
			}
		}
	}
}

func (this *SyncRedis) Close() {}

func (this *SyncRedis) finishRedisReply(offset int64) bool {
	for i := 0; i < this.appendCount; i++ {
		if r := this.client.GetReply(); r.Err != nil {
			l4g.Error("redis hmset op error: %s", r.Err.Error())
			return false
		}
	}
	if err := gldb.Put(this.leveldbKey, []byte(strconv.FormatInt(offset, 10))); err != nil {
		l4g.Error("leveldb put %s %d error: %s", this.leveldbKey, offset, err.Error())
		return false
	}
	this.appendCount = 0
	return true
}

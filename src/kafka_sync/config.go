package main

import (
	"fmt"
	"strconv"
	"strings"
)

type xmlConfig struct {
	SrcList string     `xml:"src"`
	DstList string     `xml:"dst"`
	RedisIp string     `xml:"redis"`
	Log     string     `xml:"log"`
	LevelDB xmlLevelDB `xml:"leveldb"`
	Kafka   xmlKafka   `xml:"kafka"`
	Topic   xmlTopic   `xml:"topic"`
}

type xmlTopic struct {
	Id         string `xml:"id,attr"`
	Partitions string `xml:"partitions"`
}

type xmlLevelDB struct {
	CacheSize       int    `xml:"cache_size"`
	BlockSize       int    `xml:"block_size"`
	WriteBufferSize int    `xml:"write_buffer_size"`
	MaxOpenFiles    int    `xml:"max_open_files"`
	Dir             string `xml:"dir"`
}

type xmlKafka struct {
	ConsumerFetchSize      int32 `xml:"consumer_fetch_size"`
	ConsumerFetchMinSize   int32 `xml:"consumer_fetch_min_size"`
	ConsumerNetReadTimeout int   `xml:"consumer_net_read_timeout"`
	ProducerFlushSize      int   `xml:"producer_flush_size"`
}

func (this *xmlConfig) SplitPartition() (int32, int32) {
	ss := strings.Split(this.Topic.Partitions, "-")
	if len(ss) != 2 {
		panic(fmt.Sprintf("config topic partition error: %s", this.Topic.Partitions))
	}
	min, min_err := strconv.Atoi(ss[0])
	if min_err != nil {
		panic(fmt.Sprintf("config topic partition error: %s", this.Topic.Partitions))
	}

	max, max_err := strconv.Atoi(ss[1])
	if max_err != nil {
		panic(fmt.Sprintf("config topic partition error: %s", this.Topic.Partitions))
	}

	if max < min {
		panic(fmt.Sprintf("config topic partition error: %s", this.Topic.Partitions))
	}

	return int32(min), int32(max)
}

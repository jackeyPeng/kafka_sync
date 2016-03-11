package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
)

type xmlSource struct {
	Brokers                string   `xml:"brokers"`
	Topic                  xmlTopic `xml:"topic"`
	ConsumerFetchSize      int32    `xml:"consumer_fetch_size"`
	ConsumerFetchMinSize   int32    `xml:"consumer_fetch_min_size"`
	ConsumerNetReadTimeout int      `xml:"consumer_net_read_timeout"`
}

type xmlTopic struct {
	Id         string `xml:"id"`
	Partitions string `xml:"partitions"`
}

type xmlLevelDB struct {
	CacheSize       int    `xml:"cache_size"`
	BlockSize       int    `xml:"block_size"`
	WriteBufferSize int    `xml:"write_buffer_size"`
	MaxOpenFiles    int    `xml:"max_open_files"`
	Dir             string `xml:"dir"`
}

func (this *xmlConfig) SplitSourceTopicPartition() (int32, int32) {
	ss := strings.Split(this.Source.Topic.Partitions, "-")
	if len(ss) != 2 {
		panic(fmt.Sprintf("config topic partition error: %s", this.Source.Topic.Partitions))
	}
	min, min_err := strconv.Atoi(ss[0])
	if min_err != nil {
		panic(fmt.Sprintf("config topic partition error: %s", this.Source.Topic.Partitions))
	}

	max, max_err := strconv.Atoi(ss[1])
	if max_err != nil {
		panic(fmt.Sprintf("config topic partition error: %s", this.Source.Topic.Partitions))
	}

	if max < min {
		panic(fmt.Sprintf("config topic partition error: %s", this.Source.Topic.Partitions))
	}

	return int32(min), int32(max)
}

func (this *xmlConfig) Check() bool {
	if this.GetDestinationName() != "" {
		return true
	}
	return false
}

//*********************************************************************************/
type xmlConfig struct {
	Source  xmlSource  `xml:"source"`
	Log     string     `xml:"log"`
	RedisIp string     `xml:"redis"`
	LevelDB xmlLevelDB `xml:"leveldb"`

	Destination xmlDestination `xml:"destination"`
}

func (this *xmlConfig) GetDestinationName() string {
	//kafka
	if this.Destination.Kafka.State == "open" {
		return this.Destination.Kafka.Name
	}
	//hbase
	if this.Destination.Hbase.State == "open" {
		return this.Destination.Hbase.Name
	}
	//...
	return ""
}

type xmlDestination struct {
	Kafka xmlDestinationKafka `xml:"kafka"`
	Hbase xmlDestinationHbase `xml:"hbase"`
}

//kafka config
type xmlDestinationKafka struct {
	State string `xml:"state,attr"`
	Name  string `xml:"name"`

	Brokers                string `xml:"brokers"`
	Topic                  string `xml:"topic"`
	ProducerFlushSize      int    `xml:"producer_flush_size"`
	ProducerFlushFrequency int    `xml:"producer_flush_frequency"`
}

//hbase config
type xmlDestinationHbase struct {
	State string `xml:"state,attr"`
	Name  string `xml:"name"`

	Brokers     string `xml:"brokers"`
	Table       []byte `xml:"table"`
	Family      []byte `xml:"family"`
	Qualifier   []byte `xml:"qualifier"`
	MaxMergeMsg int    `xml:"max_merge_msg"`
	Filter      string `xml:"filter"`
}

func (this *xmlDestinationHbase) GetRandomAddr() string {
	ss := strings.Split(this.Brokers, ",")
	return ss[rand.Intn(len(ss))]
}

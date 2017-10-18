package main

import (
	"math/rand"
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
	Id         string        `xml:"id"`
	Partitions xmlPartitions `xml:"partitions"`
}

type xmlPartitions struct {
	Infos []xmlPartition `xml:"partition"`
}

type xmlPartition struct {
	Index  int32 `xml:"index,attr"`
	Offset int64 `xml:",chardata"`
}

type xmlLevelDB struct {
	CacheSize       int    `xml:"cache_size"`
	BlockSize       int    `xml:"block_size"`
	WriteBufferSize int    `xml:"write_buffer_size"`
	MaxOpenFiles    int    `xml:"max_open_files"`
	Dir             string `xml:"dir"`
}

func (this *xmlConfig) Check() bool {
	uniq := make(map[int32]struct{})
	for _, info := range this.Source.Topic.Partitions.Infos {
		if _, exist := uniq[info.Index]; exist {
			return false
		}
		uniq[info.Index] = struct{}{}
	}
	if this.GetDestinationName() == "" {
		return false
	}
	return true
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
	//redis
	if this.Destination.Redis.State == "open" {
		return this.Destination.Redis.Name
	}
	//...
	return ""
}

type xmlDestination struct {
	Kafka xmlDestinationKafka `xml:"kafka"`
	Hbase xmlDestinationHbase `xml:"hbase"`
	Redis xmlDestinationRedis `xml:"redis"`
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

	TimeOut     int    `xml:"timeout"`
	Brokers     string `xml:"brokers"`
	Table       []byte `xml:"table"`
	Family      []byte `xml:"family"`
	Qualifier   []byte `xml:"qualifier"`
	MaxMergeMsg int    `xml:"max_merge_msg"`
	Filter      string `xml:"filter"`
	WriteRate   int    `xml:"write_rate"`
}

func (this *xmlDestinationHbase) GetRandomAddr() string {
	ss := strings.Split(this.Brokers, ",")
	return ss[rand.Intn(len(ss))]
}

//redis config
type xmlDestinationRedis struct {
	State string `xml:"state,attr"`
	Name  string `xml:"name"`

	LogType     string `xml:"log_type"`
	TimeOut     int    `xml:"timeout"`
	NetAddr     string `xml:"net_addr"`
	MaxMergeMsg int    `xml:"max_merge_msg"`
	WriteRate   int    `xml:"write_rate"`
}

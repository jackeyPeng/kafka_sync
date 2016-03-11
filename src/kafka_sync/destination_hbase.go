package main

import (
	"fmt"
	"strconv"
	"time"

	"base/hbase"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/Shopify/sarama"

	l4g "base/log4go"
)

const (
	TICKER_MAX_COUNT = 6
)

type Hbaser interface {
	RowKey([]byte) string
	EncodeTPut([]byte, []byte) *hbase.TPut
}

type MyHbase struct {
	cfg *xmlDestinationHbase
}

type SyncHbase struct {
	src        *SourceKafka
	leveldbKey []byte

	config     *xmlDestinationHbase
	thriftAddr string
	Hbaser
}

func NewSyncHbase(src *SourceKafka, config *xmlConfig) Syncer {
	ret := &SyncHbase{
		src:    src,
		config: &config.Destination.Hbase,
	}
	ret.leveldbKey = []byte(fmt.Sprintf("%s_%d", ret.config.Name, ret.src.PartitionIndex))
	ret.thriftAddr = ret.config.GetRandomAddr()
	ret.Hbaser = &MyHbase{
		cfg: ret.config,
	}
	return ret
}

func (this *SyncHbase) GetLevelDBKey() []byte {
	return this.leveldbKey
}

func (this *SyncHbase) Process(cc <-chan struct{}) {
	socket, err := thrift.NewTSocket(this.thriftAddr)
	if err != nil {
		l4g.Error("connect hbase proxy error: %s %s", this.thriftAddr, err.Error())
		return
	}
	defer socket.Close()

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	tt := hbase.NewTHBaseServiceClientFactory(socket, protocolFactory)
	if err := socket.Open(); err != nil {
		l4g.Error("connect hbase proxy error: %s %s", this.thriftAddr, err.Error())
		return
	}

	offset, _ := this.src.GetLevelDBOffset(this.leveldbKey)
	l4g.Debug("sync hbase leveldb offset: %s %s %d", this.thriftAddr, this.leveldbKey, offset)
	ticker := time.NewTicker(10 * time.Second)
	tps := make([]*hbase.TPut, 0, this.config.MaxMergeMsg)
	count := 0
	tickerCount := 0
	lastPutCount, putCount := 0, 0

	for {
		select {
		case <-cc:
			l4g.Info("manager hbase close %d", this.src.PartitionIndex)
			return
		case <-ticker.C:
			if count > 0 {
				if !this.PutHbase(tt, tps, offset) {
					return
				}
				count = 0
				tps = tps[0:0]
			}
			tickerCount++
			if tickerCount == TICKER_MAX_COUNT {
				l4g.Info("consume offset: %s %d %d", this.src.Topic(), this.src.PartitionIndex, putCount-lastPutCount)
				lastPutCount = putCount
				tickerCount = 0
			}
		case err := <-this.src.PartitionConsumer.Errors():
			if kerr, ok := err.Err.(sarama.KError); ok && kerr == sarama.ErrOffsetOutOfRange {
				l4g.Error("partition consumer (%s %d) return error: %s", this.src.Topic(), this.src.PartitionIndex, err.Error())
			}
			return
		case msg := <-this.src.PartitionConsumer.Messages():
			offset = msg.Offset
			key := this.RowKey(msg.Value)
			if key != "" {
				tps = append(tps, this.EncodeTPut([]byte(key), msg.Value))
				count++
				putCount++
			}
			if count == this.config.MaxMergeMsg {
				if !this.PutHbase(tt, tps, offset) {
					return
				}
				count = 0
				tps = tps[0:0]
			}
		}
	}
}

func (this *SyncHbase) Close() {}

func (this *SyncHbase) PutHbase(tt *hbase.THBaseServiceClient, tps []*hbase.TPut, offset int64) bool {
	err := tt.PutMultiple(this.config.Table, tps)
	if err != nil {
		switch v := err.(type) {
		case *hbase.TIOError:
			l4g.Error("put hbase TIOError: %s", v.GetMessage())
		case *hbase.TIllegalArgument:
			l4g.Error("put hbase TIllegalArgument: %s", v.GetMessage())
		default:
			l4g.Error("put hbase error: %d %s", err.Error())
		}
		return false
	} else {
		if err := gldb.Put(this.leveldbKey, []byte(strconv.FormatInt(offset, 10))); err != nil {
			l4g.Error("leveldb put %s %d error: %s", this.leveldbKey, offset, err.Error())
			return false
		}
	}
	return true
}

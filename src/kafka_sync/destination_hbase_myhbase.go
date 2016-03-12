package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"base/hbase"
)

type MyHbase struct {
	cfg *xmlDestinationHbase
}

func NewSyncMyHbase(src *SourceKafka, config *xmlConfig) Syncer {
	syn := NewSyncHbase(src, config)
	my := &MyHbase{
		cfg: &config.Destination.Hbase,
	}
	syn.(*SyncHbase).Hbaser = my
	return syn
}

func (this *MyHbase) RowKey(msgInfo []byte) string {
	ss := strings.Split(string(msgInfo), ",")
	if len(ss) >= 28 {
		if strings.Index(ss[0], this.cfg.Filter) == 0 {
			tm, _ := strconv.ParseUint(ss[27], 10, 64)
			res := math.MaxUint64 - tm
			rowKey := fmt.Sprintf("%s_%s_%d", ss[0], ss[3], res)
			return rowKey
		}
	}
	return ""
}

func (this *MyHbase) EncodeTPut(rowKey, value []byte) *hbase.TPut {
	tc := &hbase.TColumnValue{
		Family:    this.cfg.Family,
		Qualifier: this.cfg.Qualifier,
		Value:     value,
	}
	tp := &hbase.TPut{
		Row:          rowKey,
		ColumnValues: []*hbase.TColumnValue{tc},
	}
	return tp
}

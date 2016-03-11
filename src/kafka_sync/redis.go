package main

import (
	"fmt"
	"net"

	"github.com/ivanabc/radix/redis"
	"github.com/ivanabc/radix/redis/resp"

	l4g "base/log4go"
)

func RedisService(config *xmlConfig, sm *SyncManager) {
	addr, err := net.ResolveTCPAddr("tcp", config.RedisIp)
	if err != nil {
		panic(err.Error())
	}
	l, e := net.ListenTCP("tcp", addr)
	if e != nil {
		panic(e.Error())
	}

	defer l.Close()

	for {
		rw, e := l.AcceptTCP()
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				continue
			}
			l4g.Error("redis server accept tcp error: %s", e.Error())
			return
		}
		go RedisProcess(rw, config, sm)
	}
}

func RedisProcess(rw net.Conn, config *xmlConfig, sm *SyncManager) {
	l4g.Info("redis client init: %s", rw.(*net.TCPConn).RemoteAddr())
	client := redis.NewClient(rw)
	defer func() {
		client.Close()
		l4g.Info("redis client close: %s", rw.(*net.TCPConn).RemoteAddr())
	}()

	for {
		reply := client.ReadReply()
		infos, err := reply.Hash()
		if err != nil {
			l4g.Error("redis parse reply error: %s", err.Error())
			return
		}

		l4g.Info("redis msg: %v", infos)

		v := infos["info"]
		if v == "offset" {
			//ABOUT OFFSET
			ret := make(map[int32]string)
			skafka := &SourceKafka{
				config: &config.Source,
			}
			skafka.Init(nil, false)
			index, MaxPartition := config.SplitSourceTopicPartition()
			for index <= MaxPartition {
				skafka.PartitionIndex = index
				syn := sm.CreateSync(skafka, config)
				oldest, newest := skafka.GetKafkaOffset()
				current, _ := skafka.GetLevelDBOffset(syn.GetLevelDBKey())
				ret[index] = fmt.Sprintf("%d_%d_%d", oldest, current, newest-1)
				index++
			}
			skafka.Close()
			if err := resp.WriteArbitraryAsFlattenedStrings(rw, ret); err != nil {
				l4g.Error("write msg error: %s %v", err.Error(), ret)
			}
		} else {
			l4g.Error("parse redis error: %s %v", rw.(*net.TCPConn).RemoteAddr(), infos)
			if err := client.WriteByte([]byte("-param error\r\n")); err != nil {
				l4g.Error("redis write error: %s", err.Error())
			}
			return
		}
	}
}

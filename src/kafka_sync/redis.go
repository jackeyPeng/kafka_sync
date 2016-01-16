package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/ivanabc/radix/redis"
	"github.com/ivanabc/radix/redis/resp"

	l4g "base/log4go"
)

func RedisService(ldb *LevelDB, config *xmlConfig) {
	addr, err := net.ResolveTCPAddr("tcp", config.RedisIp)
	if err != nil {
		panic(err.Error())
	}
	l, e := net.ListenTCP("tcp", addr)
	if e != nil {
		panic(err.Error())
	}

	defer l.Close()

	for {
		rw, e := l.AcceptTCP()
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				continue
			}
			l4g.Error("redis server accept tcp error: %s", err.Error())
			return
		}
		go RedisProcess(rw, ldb, config)
	}
}

func RedisProcess(rw net.Conn, ldb *LevelDB, config *xmlConfig) {
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
			consumerClient, cErr := sarama.NewClient(strings.Split(config.SrcList, ","), nil)
			if cErr != nil {
				l4g.Error("new client error: %s %s", config.SrcList, cErr.Error())
				return
			}
			index, MaxPartition := config.SplitPartition()
			for index <= MaxPartition {
				oldest, current, newest := getKafkaOffset(consumerClient, ldb, config.Topic.Id, index, config.SrcList)
				ret[index] = fmt.Sprintf("%d_%d_%d", oldest, current, newest)
				index++
			}
			if err := consumerClient.Close(); err != nil {
				l4g.Error("close client error: %s", err.Error())
			}
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

func getKafkaOffset(consumerClient sarama.Client, ldb *LevelDB, topic string, partition int32, consumerList string) (oldest int64, current int64, newest int64) {
	oldest, _ = consumerClient.GetOffset(topic, partition, sarama.OffsetOldest)
	newest, _ = consumerClient.GetOffset(topic, partition, sarama.OffsetNewest)

	leveldbKey := []byte(fmt.Sprintf("%s_%d", topic, partition))
	ret, err := ldb.Get(leveldbKey)
	if err != nil {
		l4g.Error("leveldb get (%s %d) error: %s", topic, partition, err.Error())
		return
	}
	if len(ret) > 0 {
		current, _ = strconv.ParseInt(string(ret), 10, 64)
	} else {
		current = sarama.OffsetNewest
	}
	return
}

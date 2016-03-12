package main

import (
	"encoding/xml"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/Shopify/sarama"

	l4g "base/log4go"

	_ "net/http/pprof"
)

var gldb *LevelDB

var configFile = flag.String("config", "../config/kafka_sync_config.xml", "")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	config := new(xmlConfig)

	if contents, err := ioutil.ReadFile(*configFile); err != nil {
		panic(fmt.Sprintf("read config %v fail: %v", *configFile, err))
	} else {
		if err = xml.Unmarshal(contents, config); err != nil {
			panic(fmt.Sprintf("xml config unmarshal fail: %v", err))
		}
	}

	l4g.LoadConfiguration(config.Log)
	defer func() {
		l4g.Info("server stop...")
		l4g.Close()
	}()

	if !config.Check() {
		l4g.Info("config error")
		return
	}

	l4g.Debug("config %v", config)

	go func() {
		l4g.Error(http.ListenAndServe(":19999", nil))
	}()

	ldb, err := NewLevelDB(config)
	if err != nil {
		return
	}
	gldb = ldb
	defer gldb.Close()

	index, MaxPartition := config.SplitSourceTopicPartition()

	sarama.Logger = log.New(os.Stdout, "[Ivan] ", log.LstdFlags)

	sm := NewSyncManager()
	defer sm.Wait()

	/**************************register******************/
	//register kafka
	sm.RegisterSync(config.Destination.Kafka.Name, NewSyncKafka)
	//register hbase
	sm.RegisterSync(config.Destination.Hbase.Name, NewSyncMyHbase)
	/*****************************************************/

	sm.AddWaitGroup(int(MaxPartition - index + 1))

	l4g.Debug("partition: %d %d", index, MaxPartition)

	for index <= MaxPartition {
		sm.Create(config, index)
		index++
	}

	go RedisService(config, sm)

	sign := make(chan os.Signal, 1)
	signal.Notify(sign, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)

	select {
	case sig := <-sign:
		l4g.Info("Signal: %s", sig.String())
		sm.Close()
	case <-sm.CloseChan:
		l4g.Info("manager close")
	}
}

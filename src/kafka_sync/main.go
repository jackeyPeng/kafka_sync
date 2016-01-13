package main

import (
	"encoding/xml"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/Shopify/sarama"

	l4g "base/log4go"
)

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
	defer l4g.Close()

	ldb, err := NewLevelDB(config)
	if err != nil {
		return
	}
	defer ldb.Close()

	index, MaxPartition := config.SplitPartition()

	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)

	sm := NewSyncManager()
	defer sm.Wait()

	sm.Add(int(MaxPartition - index + 1))

	l4g.Debug("partition: %d %d", index, MaxPartition)

	for index <= MaxPartition {
		sm.Create(NewKafkaSync(ldb, config, index))
		index++
	}

	go RedisService(ldb, config)

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

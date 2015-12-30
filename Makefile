all: kafka_sync

kafka_sync:
	cd bin && GOPATH=$(PWD) CGO_CFLAGS="-I$(PWD)/deps/include" CGO_LDFLAGS="-L$(PWD)/deps/libs" go build kafka_sync

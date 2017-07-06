GOPATH = $(PWD)
all: kafka_sync

kafka_sync:
	export GOPATH=$(PWD) && cd bin && CGO_CFLAGS="-I$(GOPATH)/deps/include" CGO_LDFLAGS="-L$(GOPATH)/deps/libs" go build kafka_sync

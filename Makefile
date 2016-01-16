GOPATH = $(PWD)
all: kafka_sync

kafka_sync:
	cd bin && CGO_CFLAGS="-I$(GOPATH)/deps/include" CGO_LDFLAGS="-L$(GOPATH)/deps/libs" go build kafka_sync

#!/bin/bash

SNAPPY="snappy-1.1.1"
LEVELDB="leveldb-1.18"
DIR=$(pwd)

export GOPATH=$DIR 

if [ -d ${DIR}/deps/include ]
then
  rm -rf ${DIR}/deps/include
fi

if [ -d ${DIR}/deps/libs ]
then
  rm -rf ${DIR}/deps/libs
fi

if [ -d ${DIR}/deps/${SNAPPY} ]
then
  rm -rf ${DIR}/deps/${SNAPPY}
fi

if [ -d ${DIR}/deps/${LEVELDB} ]
then
  rm -rf ${DIR}/deps/${LEVELDB}
fi

cd ${DIR}/deps && mkdir libs
tar -zxf ${SNAPPY}.tar.gz 
cd ${DIR}/deps/${SNAPPY} 
./configure --disable-shared --with-pic && make || exit 1
cp ${DIR}/deps/${SNAPPY}/.libs/libsnappy.a ${DIR}/deps/libs

export LIBRARY_PATH=${DIR}/deps/libs
export C_INCLUDE_PATH=${DIR}/deps/${SNAPPY}
export CPLUS_INCLUDE_PATH=${DIR}/deps/${SNAPPY}

cd ${DIR}/deps
tar -zxf ${LEVELDB}.tar.gz 
cd ${DIR}/deps/${LEVELDB} 
make || exit 1
cp libleveldb.a ${DIR}/deps/libs
mv include ${DIR}/deps

cd ${DIR}

echo "go get..."
go get github.com/Shopify/sarama
go get github.com/ivanabc/radix/redis
tar -xjf deps/thrift.tar.bz2

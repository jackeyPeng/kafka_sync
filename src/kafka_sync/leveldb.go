package main

import (
	"base/levigo"

	l4g "base/log4go"
)

type LevelDB struct {
	options  *levigo.Options
	cache    *levigo.Cache
	roptions *levigo.ReadOptions
	woptions *levigo.WriteOptions
	db       *levigo.DB
}

func NewLevelDB(conf *xmlConfig) (*LevelDB, error) {
	name := conf.LevelDB.Dir
	options := levigo.NewOptions()

	options.SetCreateIfMissing(true)

	cache := levigo.NewLRUCache(conf.LevelDB.CacheSize * 1024 * 1024)
	options.SetCache(cache)

	options.SetBlockSize(conf.LevelDB.BlockSize * 1024)
	options.SetWriteBufferSize(conf.LevelDB.WriteBufferSize * 1024 * 1024)
	options.SetMaxOpenFiles(conf.LevelDB.MaxOpenFiles)
	options.SetCompression(levigo.SnappyCompression)

	filter := levigo.NewBloomFilter(10)
	options.SetFilterPolicy(filter)

	roptions := levigo.NewReadOptions()
	roptions.SetFillCache(true)

	woptions := levigo.NewWriteOptions()
	woptions.SetSync(true)

	db, err := levigo.Open(name, options)

	if err != nil {
		l4g.Error("open db failed, path: %s err: %s", name, err.Error())
		return nil, err
	}
	l4g.Info("open db succeed, path: %s", name)
	ret := &LevelDB{options,
		cache,
		roptions,
		woptions,
		db}

	return ret, nil
}

func (this *LevelDB) Put(key, value []byte) error {
	return this.db.Put(this.woptions, key, value)
}

func (this *LevelDB) Get(key []byte) ([]byte, error) {
	return this.db.Get(this.roptions, key)
}

func (this *LevelDB) Delete(key []byte) error {
	return this.db.Delete(this.woptions, key)
}

func (this *LevelDB) NewIterator() *levigo.Iterator {
	return this.db.NewIterator(this.roptions)
}

func (this *LevelDB) NewIteratorWithReadOptions(roptions *levigo.ReadOptions) *levigo.Iterator {
	return this.db.NewIterator(roptions)
}

func (this *LevelDB) Close() {
	if this.db != nil {
		this.db.Close()
	}

	if this.roptions != nil {
		this.roptions.Close()
	}

	if this.woptions != nil {
		this.woptions.Close()
	}

	if this.cache != nil {
		this.cache.Close()
	}

	if this.options != nil {
		this.options.Close()
	}
}

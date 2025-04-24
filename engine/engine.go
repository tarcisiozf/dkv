package engine

import "github.com/rosedblabs/rosedb/v2"

type DbEngine struct {
	kv *rosedb.DB
}

func NewDbEngine() (*DbEngine, error) {
	options := rosedb.DefaultOptions
	options.DirPath = "./db/"

	kv, err := rosedb.Open(options)
	if err != nil {
		return nil, err
	}

	return &DbEngine{
		kv: kv,
	}, nil
}

func (db *DbEngine) Get(key []byte) ([]byte, error) {
	return db.kv.Get(key)
}

func (db *DbEngine) Set(key, value []byte) error {
	return db.kv.Put(key, value)
}

func (db *DbEngine) Delete(key []byte) error {
	return db.kv.Delete(key)
}

func (db *DbEngine) Shutdown() {
	db.kv.Close()
}

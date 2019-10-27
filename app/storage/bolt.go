package storage

import (
	"bytes"
	"encoding/binary"
	"strings"
	"time"

	bolt "github.com/coreos/bbolt"
)

const (
	allID     = "all"
	separator = "_"
)

// StatAggregation desribes model to store into BoltDB
type StatAggregation struct {
	Fname    string
	DestHost string
	Count    int
	TS       time.Time
}

// aggregationKey generates key to get aggregation data
func (a *StatAggregation) aggregationKey() string {
	if a.Fname == "" {
		a.Fname = allID
	}
	if a.DestHost == "" {
		a.DestHost = allID
	}

	return strings.Join([]string{a.Fname, a.DestHost}, separator)
}

// Keys generate keys that should be updated by StatAggregation
func (a *StatAggregation) Keys() []string {
	recordKey := []string{a.Fname, a.DestHost}
	allFnameKey := []string{allID, a.DestHost}
	allDestKey := []string{a.Fname, allID}
	allKey := []string{allID, allID}

	return []string{
		strings.Join(recordKey, separator),
		strings.Join(allFnameKey, separator),
		strings.Join(allDestKey, separator),
		strings.Join(allKey, separator),
	}
}

const (
	// TimeFormat represents time format for key of records
	TimeFormat = "2006-01-02 15:04"
)

type PersistentStore interface {
	Save(agr StatAggregation, currentTime time.Time) error
	Get(agr StatAggregation, bucketName string, from, to time.Time) (agrs []StatAggregation, err error)
}

// Bolt describes boltd storage
type Bolt struct {
	db      *bolt.DB
	buckets []string
}

// NewBolt makes persistent boltdb based store
func NewBolt(dbFile string, buckets []string) (result *Bolt, e error) {
	db, err := bolt.Open(dbFile, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, e
	}

	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range buckets {
			_, e := tx.CreateBucketIfNotExists([]byte(name))
			if e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	result = &Bolt{}
	result.db = db
	result.buckets = buckets

	return result, nil
}

// Save aggregation to all buckets with
func (b *Bolt) Save(agr StatAggregation, currentTime time.Time) error {
	for _, name := range b.buckets {
		duration, err := time.ParseDuration(name)
		if err != nil {
			return err
		}
		timeKey := currentTime.Truncate(duration).Format(TimeFormat)

		err = b.db.Update(func(tx *bolt.Tx) error {
			root := tx.Bucket([]byte(name))
			timeBucket, e := root.CreateBucketIfNotExists([]byte(timeKey))
			if e != nil {
				return e
			}

			for _, key := range agr.Keys() {
				e = b.updateAggregate(timeBucket, []byte(key), agr.Count)
				if e != nil {
					return e
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Get updates return aggregations for given timperiod
func (b *Bolt) Get(agr StatAggregation, bucketName string, from, to time.Time) (agrs []StatAggregation, err error) {
	err = b.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(bucketName))
		c := root.Cursor()

		min := []byte(from.Format(TimeFormat))
		max := []byte(to.Format(TimeFormat))

		for bktName, _ := c.Seek(min); bktName != nil && bytes.Compare(bktName, max) <= 0; bktName, _ = c.Next() {
			bkt := root.Bucket(bktName)
			count := btoi(bkt.Get([]byte(agr.aggregationKey())))
			if count > 0 {
				agr.Count = count
				agr.TS, _ = time.Parse(TimeFormat, string(bktName))
				agrs = append(agrs, agr)
			}
		}
		return nil
	})

	return agrs, err
}

func (b *Bolt) updateAggregate(bkt *bolt.Bucket, key []byte, value int) (err error) {
	v := bkt.Get(key)
	value += btoi(v)
	return bkt.Put(key, itob(value))
}

func itob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func btoi(v []byte) int {
	if v == nil {
		return 0
	}
	return int(binary.BigEndian.Uint64(v))
}

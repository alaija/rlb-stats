package storage

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	bucket1m  = "1m"
	bucket15m = "15m"
	bucket1h  = "1h"
	bucket1d  = "24h"

	testDB = "test-bolt.db"
)

// creates new boltdb
func prepare(t *testing.T) *Bolt {
	os.Remove(testDB)

	buckets := []string{
		bucket1m,
		bucket15m,
		bucket1h,
		bucket1d,
	}

	boltStore, err := NewBolt(testDB, buckets)
	assert.Nil(t, err)

	agr := StatAggregation{
		Fname:    fname,
		DestHost: host,
		Count:    1,
	}

	agr2 := StatAggregation{
		Fname:    fname1,
		DestHost: host,
		Count:    2,
	}

	agr3 := StatAggregation{
		Fname:    fname2,
		DestHost: host1,
		Count:    3,
	}

	err = boltStore.Save(agr, time.Date(2017, 12, 20, 13, 0, 0, 0, time.Local))
	assert.Nil(t, err)

	err = boltStore.Save(agr2, time.Date(2017, 12, 20, 15, 0, 0, 0, time.Local))
	assert.Nil(t, err)

	err = boltStore.Save(agr3, time.Date(2017, 12, 20, 15, 0, 0, 0, time.Local))
	assert.Nil(t, err)

	return boltStore
}
func Test_Bolt_All(t *testing.T) {
	defer os.Remove(testDB)
	s := prepare(t)

	resultAggregation := StatAggregation{}

	result, err := s.Get(
		resultAggregation,
		bucket1m,
		time.Date(2017, 12, 20, 14, 0, 0, 0, time.Local),
		time.Date(2017, 12, 20, 16, 0, 0, 0, time.Local),
	)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 5, result[0].Count)

	result, err = s.Get(
		resultAggregation,
		bucket15m,
		time.Date(2017, 12, 20, 14, 0, 0, 0, time.Local),
		time.Date(2017, 12, 20, 16, 0, 0, 0, time.Local),
	)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 5, result[0].Count)

	result, err = s.Get(
		resultAggregation,
		bucket15m,
		time.Date(2017, 12, 20, 0, 0, 0, 0, time.Local),
		time.Date(2017, 12, 20, 14, 0, 0, 0, time.Local),
	)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 1, result[0].Count)
	result, err = s.Get(
		resultAggregation,
		bucket1d,
		time.Date(2017, 12, 20, 0, 0, 0, 0, time.Local),
		time.Date(2017, 12, 21, 0, 0, 0, 0, time.Local),
	)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 6, result[0].Count)
}

func Test_Bolt_Fname(t *testing.T) {
	defer os.Remove(testDB)
	s := prepare(t)

	resultAggregation := StatAggregation{}
	resultAggregation.Fname = fname

	result, err := s.Get(
		resultAggregation,
		bucket1m,
		time.Date(2017, 12, 20, 0, 0, 0, 0, time.Local),
		time.Date(2017, 12, 21, 0, 0, 0, 0, time.Local),
	)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 1, result[0].Count)
}

func Test_Bolt_DestHost(t *testing.T) {
	defer os.Remove(testDB)
	s := prepare(t)

	resultAggregation := StatAggregation{}
	resultAggregation.DestHost = host1

	result, err := s.Get(
		resultAggregation,
		bucket1m,
		time.Date(2017, 12, 20, 0, 0, 0, 0, time.Local),
		time.Date(2017, 12, 21, 0, 0, 0, 0, time.Local),
	)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 3, result[0].Count)
}

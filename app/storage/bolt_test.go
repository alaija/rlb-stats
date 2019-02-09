package storage

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const testDB = "test-bolt.db"

// creates new boltdb
func prepare(t *testing.T) *Bolt {
	os.Remove(testDB)

	buckets := []string{
		"1m",
		"15m",
		"1h",
		"24h",
	}

	boltStore, err := NewBolt(testDB, buckets)
	assert.Nil(t, err)

	agr := StatAggregation{
		Fname:    "123.mp3",
		DestHost: "https://test.com",
		Count:    1,
	}

	agr2 := StatAggregation{
		Fname:    "124.mp3",
		DestHost: "https://test.com",
		Count:    2,
	}

	agr3 := StatAggregation{
		Fname:    "1.mp3",
		DestHost: "https://test.ru",
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
		"1m",
		time.Date(2017, 12, 20, 14, 0, 0, 0, time.Local),
		time.Date(2017, 12, 20, 16, 0, 0, 0, time.Local),
	)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 5, result[0].Count)

	result, err = s.Get(
		resultAggregation,
		"15m",
		time.Date(2017, 12, 20, 14, 0, 0, 0, time.Local),
		time.Date(2017, 12, 20, 16, 0, 0, 0, time.Local),
	)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 5, result[0].Count)

	result, err = s.Get(
		resultAggregation,
		"15m",
		time.Date(2017, 12, 20, 0, 0, 0, 0, time.Local),
		time.Date(2017, 12, 20, 14, 0, 0, 0, time.Local),
	)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 1, result[0].Count)
	result, err = s.Get(
		resultAggregation,
		"24h",
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
	resultAggregation.Fname = "123.mp3"

	result, err := s.Get(
		resultAggregation,
		"1m",
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
	resultAggregation.DestHost = "https://test.ru"

	result, err := s.Get(
		resultAggregation,
		"1m",
		time.Date(2017, 12, 20, 0, 0, 0, 0, time.Local),
		time.Date(2017, 12, 21, 0, 0, 0, 0, time.Local),
	)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 3, result[0].Count)
}

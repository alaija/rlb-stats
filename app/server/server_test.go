package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"time"

	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ProcessStats(t *testing.T) {
	srv := NewRLBStatsServer("v1", 0)
	ts := httptest.NewServer(srv.routes())
	defer ts.Close()
	defer srv.Shutdown()

	reqrec := LogRecord{
		ID:       "123",
		FromIP:   "127.0.0.1",
		TS:       time.Date(2009, 11, 10, 23, 00, 00, 0, time.UTC),
		Fname:    "test.mp3",
		DestHost: "127.0.0.2",
	}

	data, err := json.Marshal(&reqrec)
	assert.Nil(t, err)

	client := &http.Client{Timeout: 5 * time.Second}

	resp, err := client.Post(ts.URL+"/stats", "application/json", bytes.NewReader([]byte{}))
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp, err = client.Post(ts.URL+"/stats", "application/json", bytes.NewReader(data))
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resprec := LogRecord{}
	err = json.NewDecoder(resp.Body).Decode(&resprec)
	assert.Nil(t, err)
	assert.Equal(t, reqrec, resprec)
}

package serialize_data

import "time"

type GetKey struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

type SaveKey struct {
	Key       string        `json:"key"`
	Value     interface{}   `json:"value"`
	Expiry    time.Duration `json:"expiry"`
	IsReplica bool
}

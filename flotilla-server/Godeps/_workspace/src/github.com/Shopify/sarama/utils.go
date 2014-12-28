package sarama

import (
	"io"
	"sort"
)

// make []int32 sortable so we can sort partition numbers
type int32Slice []int32

func (slice int32Slice) Len() int {
	return len(slice)
}

func (slice int32Slice) Less(i, j int) bool {
	return slice[i] < slice[j]
}

func (slice int32Slice) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

func dupeAndSort(input []int32) []int32 {
	ret := make([]int32, 0, len(input))
	for _, val := range input {
		ret = append(ret, val)
	}

	sort.Sort(int32Slice(ret))
	return ret
}

func withRecover(fn func()) {
	defer func() {
		handler := PanicHandler
		if handler != nil {
			if err := recover(); err != nil {
				handler(err)
			}
		}
	}()

	fn()
}

func safeAsyncClose(c io.Closer) {
	tmp := c // local var prevents clobbering in goroutine
	go withRecover(func() {
		if err := tmp.Close(); err != nil {
			Logger.Println("Error closing", tmp, ":", err)
		}
	})
}

// Encoder is a simple interface for any type that can be encoded as an array of bytes
// in order to be sent as the key or value of a Kafka message. Length() is provided as an
// optimization, and must return the same as len() on the result of Encode().
type Encoder interface {
	Encode() ([]byte, error)
	Length() int
}

// make strings and byte slices encodable for convenience so they can be used as keys
// and/or values in kafka messages

// StringEncoder implements the Encoder interface for Go strings so that you can do things like
//	producer.SendMessage(nil, sarama.StringEncoder("hello world"))
type StringEncoder string

func (s StringEncoder) Encode() ([]byte, error) {
	return []byte(s), nil
}

func (s StringEncoder) Length() int {
	return len(s)
}

// ByteEncoder implements the Encoder interface for Go byte slices so that you can do things like
//	producer.SendMessage(nil, sarama.ByteEncoder([]byte{0x00}))
type ByteEncoder []byte

func (b ByteEncoder) Encode() ([]byte, error) {
	return b, nil
}

func (b ByteEncoder) Length() int {
	return len(b)
}

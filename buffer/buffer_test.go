package buffer_test

import (
	"testing"

	"github.com/zikunw/dual-buffer/buffer"
)

func TestDualBuffer(t *testing.T) {
	numValues := 1024
	outputChan := make(chan buffer.KV, numValues)
	db := buffer.NewDualBuffer(10, func(b *buffer.Buffer) error {
		for _, kv := range *b {
			outputChan <- kv
		}
		return nil
	}, 200)

	kv := buffer.KV{}
	for i := 0; i < numValues; i++ {
		kv.Key = "key"
		kv.Value = i
		db.Write(&kv)
	}

	count := 0
	for kv := range outputChan {
		if kv.Value != count {
			t.Errorf("Expected %d, got %d", count, kv.Value)
		}
		if kv.Value == numValues-1 {
			break
		}
		count++
	}

	close(outputChan)
}

func BenchmarkDualBuffer(b *testing.B) {
	db := buffer.NewDualBuffer(1_000_000, func(b *buffer.Buffer) error {
		return nil
	}, 100)
	kv := buffer.KV{
		Key:   "key",
		Value: 0,
	}
	for i := 0; i < b.N; i++ {
		db.Write(&kv)
	}
}

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
	})

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

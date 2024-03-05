package main

import (
	"log"

	"github.com/zikunw/dual-buffer/buffer"
)

func main() {
	numValues := 64
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

	for kv := range outputChan {
		if kv.Value == numValues-1 {
			break
		}
	}

	close(outputChan)

	log.Println("Done")
}

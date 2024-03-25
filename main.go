package main

import (
	"log"
	"time"

	"github.com/zikunw/dual-buffer/buffer"
)

func main() {
	// numValues := 64
	// outputChan := make(chan buffer.KV, numValues)
	// db := buffer.NewDualBuffer(10, func(b *buffer.Buffer) error {
	// 	for _, kv := range *b {
	// 		outputChan <- kv
	// 	}
	// 	return nil
	// }, 50)

	// kv := buffer.KV{}
	// for i := 0; i < numValues; i++ {
	// 	kv.Key = "key"
	// 	kv.Value = i
	// 	db.Write(&kv)
	// 	time.Sleep(10 * time.Millisecond)
	// }

	// for kv := range outputChan {
	// 	log.Println(kv)
	// 	if kv.Value == numValues-1 {
	// 		break
	// 	}
	// }

	// close(outputChan)

	start := time.Now()
	counter := 0
	db := buffer.NewDualBuffer(1_000_000, func(b *buffer.Buffer) error {
		for i := 0; i < len(*b); i++ {
			counter++
		}
		return nil
	}, 100)
	kv := buffer.KV{
		Key:   "key",
		Value: 0,
	}
	for i := 0; i < 100_000_000; i++ {
		db.Write(&kv)
	}
	elapsed := time.Since(start)
	log.Printf("Dual Buffer Took %s", elapsed)

	start = time.Now()
	counter = 0
	c := make(chan buffer.KV, 1_000_000)
	go func() {
		for i := 0; i < 100_000_000; i++ {
			c <- kv
		}
		close(c)
	}()
	for range c {
		counter++
	}
	elapsed = time.Since(start)
	log.Printf("Channel Took %s", elapsed)

	log.Println("Done")
}

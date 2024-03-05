package main

import (
	"log"
	"time"
)

func main() {
	db := NewDualBuffer(func(b *buffer) error {
		log.Println("Processing buffer", b)
		time.Sleep(time.Duration(500) * time.Millisecond)
		return nil
	})

	kv := KV{}
	for i := 0; i < 1024; i++ {
		kv.Key = "key"
		kv.Value = i
		db.Write(&kv)
		log.Println("Writing", i)
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

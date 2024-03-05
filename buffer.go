package main

import (
	"sync"
)

const (
	BufferSize = 10
)

type KV struct {
	Key   string
	Value []byte
}

type buffer []KV

type DualBuffer struct {
	buffer1 buffer
	buffer2 buffer

	currentBuffer *buffer
	currentIndex  uint

	wg sync.WaitGroup

	// callback function to execute when buffer is full
	execFunc func(*buffer) error
}

func NewDualBuffer(execFunc func(*buffer) error) *DualBuffer {
	b1 := make(buffer, BufferSize)
	b2 := make(buffer, BufferSize)
	db := &DualBuffer{
		wg:           sync.WaitGroup{},
		buffer1:      b1,
		buffer2:      b2,
		currentIndex: 0,
		execFunc:     execFunc,
	}
	db.currentBuffer = &db.buffer1
	return db
}

func (d *DualBuffer) Write(kv *KV) error {

	if d.currentIndex >= BufferSize {
		// if we are still processing the previous buffer, wait
		d.wg.Wait()
		// process buffer
		d.wg.Add(1)
		go d.ProcessBuffer(d.currentBuffer)
		// swap buffers
		if d.currentBuffer == &d.buffer1 {
			d.currentBuffer = &d.buffer2
		} else {
			d.currentBuffer = &d.buffer1
		}
		d.currentIndex = 0
	}

	(*d.currentBuffer)[d.currentIndex] = *kv
	d.currentIndex++

	return nil
}

func (d *DualBuffer) ProcessBuffer(buffer *buffer) error {
	// process buffer
	err := d.execFunc(buffer)
	// clear buffer
	for i := 0; i < BufferSize; i++ {
		(*buffer)[i] = KV{}
	}
	d.wg.Done()
	return err
}

package buffer

import (
	"sync"
)

type KV struct {
	Key   string
	Value int
}

type Buffer []KV

type DualBuffer struct {
	buffer1 Buffer
	buffer2 Buffer

	bufferSize uint

	currentBuffer *Buffer
	currentIndex  uint

	wg sync.WaitGroup

	timeout int // timeout in milliseconds

	// callback function to execute when buffer is full
	execFunc func(*Buffer) error
}

func NewDualBuffer(
	bufferSize uint,
	execFunc func(*Buffer) error,
	timeout int,
) *DualBuffer {
	b1 := make(Buffer, bufferSize)
	b2 := make(Buffer, bufferSize)
	db := &DualBuffer{
		wg:           sync.WaitGroup{},
		buffer1:      b1,
		buffer2:      b2,
		currentIndex: 0,
		execFunc:     execFunc,
		bufferSize:   bufferSize,
		timeout:      timeout,
	}
	db.currentBuffer = &db.buffer1
	go db.timeoutProcess()
	return db
}

func (d *DualBuffer) Write(kv *KV) error {
	if d.currentIndex >= d.bufferSize {
		d.wg.Wait()
		d.wg.Add(1)
		go d.ProcessBuffer(d.currentBuffer)
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

func (d *DualBuffer) timeoutProcess() {
	// ticker := time.NewTicker(time.Duration(d.timeout) * time.Millisecond)
	// for {
	// 	<-ticker.C
	// 	if d.currentIndex > 0 {
	// 		d.wg.Wait()
	// 		d.wg.Add(1)
	// 		go d.ProcessBuffer(d.currentBuffer)
	// 		if d.currentBuffer == &d.buffer1 {
	// 			d.currentBuffer = &d.buffer2
	// 		} else {
	// 			d.currentBuffer = &d.buffer1
	// 		}
	// 		d.currentIndex = 0
	// 	}
	// }
}

func (d *DualBuffer) ProcessBuffer(buffer *Buffer) error {
	err := d.execFunc(buffer)
	for i := 0; i < int(d.bufferSize); i++ {
		(*buffer)[i] = KV{}
	}
	d.wg.Done()
	return err
}

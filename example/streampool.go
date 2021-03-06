package main

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/marlonche/connpool"
)

type PooledStreamErr string

func (self PooledStreamErr) Error() string {
	return string(self)
}

var invalidStream = PooledStreamErr("invalid pooled stream")

// Implemented PoolItem: pooled TCP stream
type PooledStream struct {
	sync.RWMutex
	stream    net.Conn
	pool      *StreamPool
	err       error
	container connpool.PoolItem
	closed    bool
}

func NewPooledStream(stream net.Conn, pool *StreamPool) *PooledStream {
	return &PooledStream{
		stream: stream,
		pool:   pool,
		closed: false,
	}
}

// Just save the parameter passed in
func (self *PooledStream) SetContainer(container connpool.PoolItem) {
	self.container = container
}

// Just return the saved parameter of SetContainer()
func (self *PooledStream) GetContainer() connpool.PoolItem {
	return self.container
}

// This method is called by connpool as well as by users who encounter errors when
// using PooledStream, e.g., PooledStream.Read().
// You can keep the error if it's unrecoverable and you want to discard the PooledStream,
// or you can ignore the error if it doesn't affect the reuse of the PooledStream.
// In PooledStream.Close(), you can check the kept error to discard or reuse PooledStream.
func (self *PooledStream) SetErr(err error) {
	self.Lock()
	defer self.Unlock()
	if self.closed {
		return
	}
	if err != nil {
		if self.err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				return
			}
		}
		self.err = err
	}
}

// Return the error set by SetErr().
func (self *PooledStream) GetErr() error {
	self.RLock()
	defer self.RUnlock()
	return self.err
}

// Called after finishing using the PooledStream.
// If the item is in error state, clear it by calling Pool.ClearItem(),
// otherwise give it back by calling Pool.GiveBack().
func (self *PooledStream) Close() error {
	self.Lock()
	defer self.Unlock()
	if self.closed {
		return invalidStream
	}
	err := self.err
	if err != nil {
		fmt.Printf("PooledStream Close, self:%p, err:%v\n", self, err)
		self.pool.clearConn(self)
		self.pool = nil
		self.closed = true
		self.stream.Close()
	} else {
		self.pool.giveBack(self)
	}
	return nil
}

// Not part of PoolItem interface, just application logic.
func (self *PooledStream) Write(b []byte) (int, error) {
	return self.stream.Write(b)
}

func (self *PooledStream) Read(b []byte) (int, error) {
	return self.stream.Read(b)
}

// Implemented PoolItem creator
type streamCreator struct {
	pool *StreamPool
	addr string
}

// Called by connpool when more PoolItems are needed.
func (self *streamCreator) NewItem() (connpool.PoolItem, error) {
	conn, err := net.Dial("tcp", self.addr)
	if err != nil {
		return nil, err
	}
	pooledStream := NewPooledStream(conn, self.pool)
	fmt.Printf("NewPooledStream:%p\n", pooledStream)
	return pooledStream, nil
}

// Called by connpool every time before Pool.Get()'s return.
// n = 1 means the first time.
func (self *streamCreator) InitItem(item connpool.PoolItem, n uint64) error {
	if 1 == n {
		// first Get()
		if stream, _ := item.(*PooledStream); stream != nil {
			// receive from stream
			go func() {
				r := bufio.NewReader(stream)
				for {
					s, err := r.ReadString('\n')
					if err != nil {
						fmt.Printf("ReadString error:%v\n", err)
						stream.SetErr(err)
						stream.Close()
						break
					}
					fmt.Printf("%v, get echo from server: %v", time.Now(), s)
				}
			}()
		}
	}
	return nil
}

func (self *streamCreator) Close() error {
	self.pool = nil
	return nil
}

// pool wrapper
type StreamPool struct {
	pool *connpool.Pool
}

func NewStreamPool(name string, addr string, maxTotal int, maxIdle int, idleTimeout int) *StreamPool {
	creator := &streamCreator{
		addr: addr,
	}
	streamPool := &StreamPool{
		pool: connpool.NewPool(name, creator, maxTotal, maxIdle, idleTimeout),
	}
	//streamPool.pool.SetGetTimeout(10)
	creator.pool = streamPool
	return streamPool
}

// wrapper of Pool.Get()
func (self *StreamPool) Get() (*PooledStream, error) {
	item, err := self.pool.Get()
	if err != nil {
		return nil, err
	}
	if stream, ok := item.(*PooledStream); ok && stream != nil {
		return stream, nil
	}
	return nil, invalidStream
}

// wrapper of Pool.ClearItem()
func (self *StreamPool) clearConn(pooledStream *PooledStream) {
	self.pool.ClearItem(pooledStream)
}

// wrapper of Pool.GiveBack()
func (self *StreamPool) giveBack(pooledStream *PooledStream) {
	self.pool.GiveBack(pooledStream)
}

// wrapper of Pool.Close()
func (self *StreamPool) Close() {
	self.pool.Close()
}

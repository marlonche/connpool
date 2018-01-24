package connpool

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Pooled items should implement this interface.
type PoolItem interface {
	// Called after finishing using the PoolItem.
	// If the item is in error state, clear it by calling pool.ClearItem(),
	// otherwise give it back by calling pool.GiveBack().
	Close() error

	// Save the error if the error is not recoverable.
	// This method is called by connpool as well as by users who encounter
	// errors when using PoolItem.
	SetErr(error)
	// Return error saved previously by SetErr().
	GetErr() error

	// The two methods below are just called by connpool.
	// Implementers just need to save the parameter passed in.
	SetContainer(PoolItem)
	// Implementers just need to return the parameter saved by SetContainer().
	GetContainer() PoolItem
}

// Users should implement this interface to create PoolItems.
type Creator interface {
	// Used to create a new item which will be returned by Pool.Get().
	// It will be called if there are not enough items
	// and there is still capacity space to allocate new items.
	NewItem() (PoolItem, error)

	// This method will be called every time before Pool.Get() returning a PoolItem to user.
	// Every item returned from Get() will be initialized with this method.
	//
	// item is the one will be returned by Pool.Get().
	// n is the use count of this item.
	// n = 1 means the first use of this item.
	//
	// If the returned error is not nil, item.SetErr() and item.Close() will be
	// called sequentially by connpool, and item will not be returned by Pool.Get()
	InitItem(item PoolItem, n uint64) error
	Close() error
}

type itemInfo struct {
	item     PoolItem
	active   bool
	useCount uint64
	idleTime int64
	closed   bool
	err      error
	timer    *time.Timer
}

// The main pool struct.
type Pool struct {
	name        string
	chanIdle    chan *itemInfo
	chanToNew   chan struct{}
	chanTotal   chan struct{}
	maxTotalNum int
	maxIdleNum  int
	idleTimeout int
	getTimeout  int
	creator     Creator
	chanClose   chan struct{}
	timerPool   sync.Pool
}

var (
	ErrPoolClosed  = errors.New("the pool is closed")
	ErrIdleTimeout = errors.New("the item is idle timeout")
	ErrIdleFull    = errors.New("idle items are full")
	ErrGetTimeout  = errors.New("no item to get")
)

func newInfoItem(poolItem PoolItem) *itemInfo {
	infoItem := &itemInfo{
		item:     poolItem,
		active:   false,
		useCount: 0,
		idleTime: time.Now().Unix(),
		closed:   false,
		timer:    time.NewTimer(time.Second),
	}
	if !infoItem.timer.Stop() {
		<-infoItem.timer.C
	}
	return infoItem
}

func (self *itemInfo) Close() error {
	return self.item.Close()
}

func (self *itemInfo) SetErr(err error) {
	self.err = err
	self.item.SetErr(err)
}

func (self *itemInfo) GetErr() error {
	return self.err
}

func (self *itemInfo) GetContainer() PoolItem {
	return nil
}

func (self *itemInfo) SetContainer(container PoolItem) {
}

// Create a connection pool.
//
// name is an unique id of this pool;
//
// creator is the Creator interface implemented by user;
//
// maxTotalNum is the maximum total number of active and idle connections hold by this pool;
//
// Here active refers to an item being hold by a user after Pool.Get(),
// while idle refers to an item in the pool waiting for Pool.Get().
//
// maxIdleNum is the maximum number of idle connections hold by this pool;
//
// idleTimeout is the timeout in second of idle connections, 0 means no timeout;
// If an item is in idle state for at least idleTimeout seconds, the item will be
// closed with error ErrIdleTimeout.
func NewPool(name string, creator Creator, maxTotalNum int, maxIdleNum int, idleTimeout int) *Pool {
	fmt.Printf("NewPool, name:%v, maxTotalNum:%v, maxIdleNum:%v, idleTimeout:%v\n", name, maxTotalNum, maxIdleNum, idleTimeout)
	if maxIdleNum == maxTotalNum {
		maxIdleNum = maxTotalNum + 1 //manage to be reused
	}
	pool := &Pool{
		name:        name,
		maxTotalNum: maxTotalNum,
		maxIdleNum:  maxIdleNum,
		idleTimeout: idleTimeout,
		creator:     creator,
		chanIdle:    make(chan *itemInfo, maxIdleNum),
		chanToNew:   make(chan struct{}, 1),
		chanTotal:   make(chan struct{}, maxTotalNum),
		chanClose:   make(chan struct{}, 1),
	}
	pool.timerPool.New = func() interface{} {
		t := time.NewTimer(time.Second)
		if !t.Stop() {
			<-t.C
		}
		return t
	}
	go pool.newItem()
	go pool.checkIdle()
	return pool
}

func (self *Pool) newItem() {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("panic:%v, pool-name:%v, chanTotal closed?\n", e, self.name)
		}
	}()
	for {
		select {
		case <-self.chanClose:
			fmt.Printf("chanToNew closed, pool-name:%v\n", self.name)
			return
		case <-self.chanToNew:
		}
		select {
		case itemTemp, ok := <-self.chanIdle:
			if !ok {
				return
			}
			self.chanIdle <- itemTemp
			continue
		case self.chanTotal <- struct{}{}:
		}
		go func() {
			defer func() {
				if e := recover(); e != nil {
					fmt.Printf("panic:%v, pool-name:%v, chanIdle closed?\n", e, self.name)
				}
			}()
			item, err := self.creator.NewItem()
			if err != nil {
				<-self.chanTotal
				if len(self.chanTotal) < 1 {
					time.Sleep(time.Second * time.Duration(2))
					select {
					case <-self.chanClose:
					case self.chanToNew <- struct{}{}:
					}
				}
				fmt.Printf("creator NewItem, pool-name:%v, error:%v\n", self.name, err)
				return
			}
			itemInfo := newInfoItem(item)
			item.SetContainer(itemInfo)
			self.chanIdle <- itemInfo
			fmt.Printf("newItem item:%p, pool-name:%v, chanTotal:%v, chanToNew:%v, chanIdle:%v\n", itemInfo, self.name, len(self.chanTotal), len(self.chanToNew), len(self.chanIdle))
		}()
	}
}

func (self *Pool) checkIdle() {
	if self.idleTimeout <= 0 {
		return
	}
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("pool closed, pool-name:%v, panic:%v\n", self.name, e)
		}
	}()
	checkInterval := self.idleTimeout
	if checkInterval > 10 {
		checkInterval = 10
	}
	for {
		select {
		case itemTemp, ok := <-self.chanIdle:
			if !ok {
				return
			}
			if self.checkIdleTimeout(itemTemp) {
				continue
			}
			_t := self.timerPool.Get()
			t, _ := _t.(*time.Timer)
			if nil == t {
				t = time.NewTimer(time.Duration(self.idleTimeout) * time.Second)
			} else {
				t.Reset(time.Duration(self.idleTimeout) * time.Second)
			}
			select {
			case self.chanIdle <- itemTemp:
				if !t.Stop() {
					<-t.C
				}
				self.timerPool.Put(t)
			case <-t.C:
				self.timerPool.Put(t)
				self.closeItem(itemTemp, ErrIdleTimeout)
				continue
			}
			time.Sleep(time.Duration(checkInterval) * time.Second)
		}
	}
}

// Set Get()'s timeout in second, 0 means no timeout, default 0.
// Get() will return with error ErrGetTimeout on timeout.
//
// This method can be called after NewPool().
func (self *Pool) SetGetTimeout(timeout int) {
	self.getTimeout = timeout
}

// Get pooled item originally created by Creator.NewItem().
//
// If SetGetTimeout() is called with non-zero value, Get() will return with
// error ErrGetTimeout after timeout.
func (self *Pool) Get() (_item PoolItem, _err error) {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("pool closed, pool-name:%v, panic:%v\n", self.name, e)
			_item = nil
			_err = ErrPoolClosed
		}
	}()
	var item *itemInfo = nil
	var ok bool
	for {
		select {
		case item, ok = <-self.chanIdle:
			if !ok {
				return nil, ErrPoolClosed
			}
		default:
			select {
			case self.chanToNew <- struct{}{}:
			default:
			}
		}
		if item != nil {
			if item.closed {
				item = nil
				continue
			}
			item.active = true
			item.useCount++
			if self.checkIdleTimeout(item) {
				item = nil
				continue
			}
			if err := self.creator.InitItem(item.item, item.useCount); err != nil {
				fmt.Printf("InitItem error, item:%v, pool-name:%v, err:%v\n", item, self.name, err)
				self.closeItem(item, err)
				item = nil
				continue
			}
			return item.item, nil
		}
		if self.getTimeout > 0 {
			_t := self.timerPool.Get()
			t, _ := _t.(*time.Timer)
			if nil == t {
				t = time.NewTimer(time.Duration(self.getTimeout) * time.Second)
			} else {
				t.Reset(time.Duration(self.getTimeout) * time.Second)
			}
			select {
			case item, ok = <-self.chanIdle:
				if !t.Stop() {
					<-t.C
				}
				self.timerPool.Put(t)
				if !ok {
					return nil, ErrPoolClosed
				}
			case <-t.C:
				self.timerPool.Put(t)
				return nil, ErrGetTimeout
			}
		} else {
			item, ok = <-self.chanIdle
			if !ok {
				return nil, ErrPoolClosed
			}
		}
		select {
		case self.chanToNew <- struct{}{}:
		default:
		}
		if item != nil {
			if item.closed {
				item = nil
				continue
			}
			item.active = true
			item.useCount++
			if self.checkIdleTimeout(item) {
				item = nil
				continue
			}
			if err := self.creator.InitItem(item.item, item.useCount); err != nil {
				fmt.Printf("InitItem error, item:%v, pool-name:%v, err:%v\n", item, self.name, err)
				self.closeItem(item, err)
				item = nil
				continue
			}
			return item.item, nil
		}
	}
}

func (self *Pool) checkIdleTimeout(item *itemInfo) bool {
	if self.idleTimeout <= 0 {
		return false
	}
	if item.idleTime <= 0 {
		return false
	}
	markTime := time.Now().Unix() - int64(self.idleTimeout)
	if item.idleTime <= markTime {
		self.closeItem(item, ErrIdleTimeout)
		return true
	}
	return false
}

func (self *Pool) closeItem(item *itemInfo, err error) {
	go func() {
		item.SetErr(err)
		item.Close()
	}()
}

// Call this method to clear items with error from the pool.
//
// This method is called by user in the implementation of PoolItem.Close() when
// an error previously set by PoolItem.SetErr() is detected.
func (self *Pool) ClearItem(item PoolItem) {
	go self.doClearItem(item)
}

func (self *Pool) doClearItem(_item PoolItem) {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("panic:%v, pool-name:%v\n", e, self.name)
		}
	}()
	container := _item.GetContainer()
	if item, ok := container.(*itemInfo); ok && item != nil {
		if item.closed {
			return
		}
		item.closed = true
		<-self.chanTotal
		err := item.item.GetErr()
		item.item.SetContainer(nil)
		if err != ErrPoolClosed && err != ErrIdleFull && err != ErrIdleTimeout {
			fmt.Printf("clearItem with error to new:%v, pool-name:%v\n", err, self.name)
			select {
			case self.chanToNew <- struct{}{}:
			default:
			}
		}
	}
}

// Check whether an item is active or not.
func (self *Pool) IsItemActive(_item PoolItem) bool {
	container := _item.GetContainer()
	if item, ok := container.(*itemInfo); ok && item != nil {
		return item.active
	}
	return false
}

// Call this method to give normal(non-error) items back to the pool after finishing using.
//
// This method is called by user in the implementation of PoolItem.Close() when
// no error with item is detected.
//
// If idle items are full, this item will be closed with error ErrIdleFull.
func (self *Pool) GiveBack(item PoolItem) {
	go self.doGiveBack(item)
}

var unit uint64 = 0
var unitCount uint64 = 0

func (self *Pool) doGiveBack(_item PoolItem) {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("panic:%v, pool-name:%v\n", e, self.name)
		}
	}()
	container := _item.GetContainer()
	item, ok := container.(*itemInfo)
	if !ok || nil == item {
		fmt.Printf("invalid poolItem, pool-name:%v\n", self.name)
		return
	}
	if item.closed {
		return
	}
	item.active = false
	item.idleTime = time.Now().Unix()
	item.timer.Reset(time.Duration(10) * time.Second)
	select {
	case self.chanIdle <- item: //may send on closed channel
		if !item.timer.Stop() {
			<-item.timer.C
		}
	case <-item.timer.C:
		self.closeItem(item, ErrIdleFull)
		return
	}
	unit++
	if unit >= 200 {
		unit = 0
		unitCount++
		fmt.Printf("doGiveBack unitCount:%v, pool-name:%v\n", unitCount, self.name)
	}
}

// Close the pool.
func (self *Pool) Close() {
	fmt.Printf("Close Pool, pool-name:%v\n", self.name)
	select {
	case <-self.chanClose:
		return
	default:
	}

	close(self.chanToNew)
	close(self.chanTotal)
	close(self.chanClose)
	close(self.chanIdle)
	for item := range self.chanIdle {
		self.closeItem(item, ErrPoolClosed)
	}
	self.creator.Close()
}

// Pool closed or not.
func (self *Pool) Closed() bool {
	select {
	case <-self.chanClose:
		return true
	default:
		return false
	}
}

// Get the total number of all items including active and idle.
func (self *Pool) GetTotalNum() int {
	return len(self.chanTotal)
}

// Get the number of idle items.
func (self *Pool) GetIdleNum() int {
	return len(self.chanIdle)
}

// Get the name of pool specified at NewPool()
func (self *Pool) GetName() string {
	return self.name
}

package connpool

import (
	"errors"
	"fmt"
	"time"
)

// Pooled items should implement this interface.
type PoolItem interface {
	// Called after the PoolItem has been used.
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
	// item is the one will be returned by Get().
	//
	// n is the use count of this item.
	// n = 1 means the first use of this item.
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
}

// The main pool struct.
type Pool struct {
	name        string
	chanIdle    chan *itemInfo
	chanToNew   chan struct{}
	chanTotal   chan struct{}
	maxItemNum  int
	maxIdleNum  int
	idleTimeout int
	getTimeout  int
	creator     Creator
	chanClose   chan struct{}
}

var errPoolClosed = errors.New("the pool is closed")
var errIdleTimeout = errors.New("the item is idle timeout")
var errIdleFull = errors.New("idle items are full")
var errGetTimeout = errors.New("no item to get")

func newInfoItem(poolItem PoolItem) *itemInfo {
	infoItem := &itemInfo{
		item:     poolItem,
		active:   false,
		useCount: 0,
		idleTime: time.Now().Unix(),
		closed:   false,
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
// maxItemNum is the maximum number of active and idle connections hold by this pool;
//
// Here active refers to an item being hold by a user after Pool.Get(),
// while idle refers to an item in the pool waiting for Pool.Get().
//
// maxIdleNum is the maximum number of idle connections hold by this pool,
//
// idleTimeout is the timeout in second of idle connections, 0 means no timeout.
// If an item is in idle state for at least idleTimeout seconds, the item will be removed from pool.
func NewPool(name string, creator Creator, maxItemNum int, maxIdleNum int, idleTimeout int) *Pool {
	fmt.Printf("NewPool, name:%v, maxItemNum:%v, maxIdleNum:%v, idleTimeout:%v\n", name, maxItemNum, maxIdleNum, idleTimeout)
	if maxIdleNum == maxItemNum {
		maxIdleNum = maxItemNum + 1 //manage to be reused
	}
	pool := &Pool{
		name:        name,
		maxItemNum:  maxItemNum,
		maxIdleNum:  maxIdleNum,
		idleTimeout: idleTimeout,
		creator:     creator,
		chanIdle:    make(chan *itemInfo, maxIdleNum),
		chanToNew:   make(chan struct{}, 1),
		chanTotal:   make(chan struct{}, maxItemNum),
		chanClose:   make(chan struct{}, 1),
	}
	go pool.newItem()
	go pool.checkIdle()
	return pool
}

func (self *Pool) newItem() {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("panic:%v, chanTotal closed?\n", e)
		}
	}()
	for {
		select {
		case <-self.chanClose:
			fmt.Printf("chanToNew closed\n")
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
					fmt.Printf("panic:%v, chanIdle closed?\n", e)
				}
			}()
			item, err := self.creator.NewItem()
			if err != nil {
				<-self.chanTotal
				if len(self.chanTotal) < 5 {
					time.Sleep(time.Minute)
					select {
					case <-self.chanClose:
					case self.chanToNew <- struct{}{}:
					}
				}
				fmt.Printf("creator NewItem, self:%v, error:%v\n", self.name, err)
				return
			}
			itemInfo := newInfoItem(item)
			item.SetContainer(itemInfo)
			self.chanIdle <- itemInfo
			fmt.Printf("newItem item:%p, self:%v, chanTotal:%v, chanToNew:%v, chanIdle:%v\n", itemInfo, self.name, len(self.chanTotal), len(self.chanToNew), len(self.chanIdle))
		}()
	}
}

func (self *Pool) checkIdle() {
	if self.idleTimeout <= 0 {
		return
	}
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("pool:%v closed, panic:%v\n", self.name, e)
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
			select {
			case self.chanIdle <- itemTemp:
			case <-time.After(time.Duration(self.idleTimeout) * time.Second):
				self.closeItem(itemTemp, errIdleTimeout)
				continue
			}
			time.Sleep(time.Duration(checkInterval) * time.Second)
		}
	}
}

// Set Get()'s timeout in second, 0 means no timeout.
func (self *Pool) SetGetTimeout(timeout int) {
	self.getTimeout = timeout
}

// Get pooled item created by Creator.NewItem()
func (self *Pool) Get() (_item PoolItem, _err error) {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("pool closed, panic:%v\n", e)
			_item = nil
			_err = errPoolClosed
		}
	}()
	var item *itemInfo = nil
	var ok bool
	for {
		select {
		case item, ok = <-self.chanIdle:
			if !ok {
				return nil, errPoolClosed
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
				fmt.Printf("InitItem error, item:%v, self:%v, err:%v\n", item, self.name, err)
				self.closeItem(item, err)
				return nil, err
			}
			return item.item, nil
		}
		if self.getTimeout > 0 {
			select {
			case item, ok = <-self.chanIdle:
				if !ok {
					return nil, errPoolClosed
				}
			case <-time.After(time.Duration(self.getTimeout) * time.Second):
				return nil, errGetTimeout
			}
		} else {
			item, ok = <-self.chanIdle
			if !ok {
				return nil, errPoolClosed
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
				fmt.Printf("InitItem error, item:%v, self:%v, err:%v\n", item, self.name, err)
				self.closeItem(item, err)
				return nil, err
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
		self.closeItem(item, errIdleTimeout)
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

// Call this method to clear items in error state from the pool.
func (self *Pool) ClearItem(item PoolItem) {
	go self.doClearItem(item)
}

func (self *Pool) doClearItem(_item PoolItem) {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("panic:%v\n", e)
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
		if err != errPoolClosed && err != errIdleFull && err != errIdleTimeout {
			fmt.Printf("clearItem error to new:%v\n", err)
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

// Call this method to give normal items back to the pool after usage.
func (self *Pool) GiveBack(item PoolItem) {
	go self.doGiveBack(item)
}

var unit uint64 = 0
var unitCount uint64 = 0

func (self *Pool) doGiveBack(_item PoolItem) {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("panic:%v\n", e)
		}
	}()
	container := _item.GetContainer()
	item, ok := container.(*itemInfo)
	if !ok || nil == item {
		fmt.Printf("invalid poolItem, self:%v\n", self.name)
		return
	}
	if item.closed {
		return
	}
	item.active = false
	item.idleTime = time.Now().Unix()
	select {
	case self.chanIdle <- item: //may send on closed channel
	case <-time.After(time.Duration(10) * time.Second):
		self.closeItem(item, errIdleFull)
		return
	}
	unit++
	if unit >= 200 {
		unit = 0
		unitCount++
		fmt.Printf("doGiveBack unitCount:%v, self:%v\n", unitCount, self.name)
	}
}

// Close the pool.
func (self *Pool) Close() {
	fmt.Printf("Close Pool:%v\n", self.name)
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
		self.closeItem(item, errPoolClosed)
	}
	self.creator.Close()
}

func (self *Pool) Closed() bool {
	select {
	case <-self.chanClose:
		return true
	default:
		return false
	}
}

func (self *Pool) GetItemNum() int {
	return len(self.chanIdle)
}

func (self *Pool) GetIdleNum() int {
	return len(self.chanIdle)
}

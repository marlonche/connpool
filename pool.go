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
	name          string
	chanIdle      chan *itemInfo
	chanToNew     chan byte
	chanCheckMore chan byte
	chanTotal     chan byte
	maxItemNum    int
	maxIdleNum    int
	idleTimeout   int
	getTimeout    int
	creator       Creator
	chanClose     chan bool
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
// name is an unique id of this pool,
//
// creator is the Creator interface implemented by user,
//
// maxItemNum is the maximum number of active and idle connections hold by this pool,
//
// maxIdleNum is the maximum number of idle connections hold by this pool,
//
// idleTimeout is the timeout of idle connections in second.
func NewPool(name string, creator Creator, maxItemNum int, maxIdleNum int, idleTimeout int) *Pool {
	fmt.Printf("NewPool, name:%v, maxItemNum:%v, maxIdleNum:%v, idleTimeout:%v", name, maxItemNum, maxIdleNum, idleTimeout)
	if maxIdleNum == maxItemNum {
		maxIdleNum = maxItemNum + 1 //manage to be reused
	}
	pool := &Pool{
		name:          name,
		maxItemNum:    maxItemNum,
		maxIdleNum:    maxIdleNum,
		idleTimeout:   idleTimeout,
		creator:       creator,
		chanIdle:      make(chan *itemInfo, maxIdleNum),
		chanToNew:     make(chan byte, maxIdleNum),
		chanCheckMore: make(chan byte, maxIdleNum),
		chanTotal:     make(chan byte, maxItemNum),
		chanClose:     make(chan bool, 1),
	}
	go pool.newItem()
	go pool.checkMore()
	/*for i := 0; i < maxIdleNum; i++ {
		//pool.chanToNew <- 1
		_item, err := pool.creator.NewItem()
		if nil == err {
			pool.chanIdle <- newInfoItem(_item)
		}
	}*/
	fmt.Printf("NewPool:%v", pool.name)
	return pool
}

func (self *Pool) newItem() {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("panic:%v, chanTotal closed?", e)
		}
	}()
	for {
		select {
		case <-self.chanClose:
			fmt.Printf("chanToNew closed")
			return
		case <-self.chanToNew:
		}
		self.chanTotal <- 1
		go func() {
			defer func() {
				if e := recover(); e != nil {
					fmt.Printf("panic:%v, chanIdle closed?", e)
				}
			}()
			item, err := self.creator.NewItem()
			if err != nil {
				<-self.chanTotal
				if len(self.chanTotal) < 5 {
					time.Sleep(time.Minute)
					select {
					case <-self.chanClose:
					case self.chanToNew <- 1:
					}
				}
				fmt.Printf("creator NewItem, self:%v, error:%v", self.name, err)
				return
			}
			itemInfo := newInfoItem(item)
			item.SetContainer(itemInfo)
			self.chanIdle <- itemInfo
			fmt.Printf("newItem item:%p, self:%v, chanTotal:%v, chanToNew:%v, chanIdle:%v", itemInfo, self.name, len(self.chanTotal), len(self.chanToNew), len(self.chanIdle))
		}()
	}
}

func (self *Pool) checkMore() {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("pool:%v closed, panic:%v", self.name, e)
		}
	}()
	for {
		<-self.chanCheckMore
		select {
		case itemTemp, ok := <-self.chanIdle:
			if !ok {
				return
			}
			self.chanIdle <- itemTemp
		default:
			self.chanToNew <- 1
		}
	}
}

// Set Get()'s timeout in second, 0 means no timeout.
func (self *Pool) SetGetTimeout(timeout int) {
	self.getTimeout = timeout
}

// Get pooled item created by Creator.NewItem()
func (self *Pool) Get() (_item PoolItem, _err error) {
	/*item, ok := <-self.chanIdle
	if ok {
		//fmt.Printf("Get item:%p", item)
		return item, nil
	}
	return nil, errors.New("closed")
	*/

	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("pool closed, panic:%v", e)
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
			case self.chanToNew <- 1:
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
				fmt.Printf("InitItem error, item:%v, self:%v, err:%v", item, self.name, err)
				self.closeItem(item, err)
				return nil, err
			}
			//fmt.Printf("Get item:%p", item)
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
		case self.chanCheckMore <- 1:
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
				fmt.Printf("InitItem error, item:%v, self:%v, err:%v", item, self.name, err)
				self.closeItem(item, err)
				return nil, err
			}
			//fmt.Printf("Get item:%p", item)
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
	fmt.Printf("idleTime:%v, markTime:%v", item.idleTime, markTime)
	if item.idleTime < markTime {
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
			fmt.Printf("panic:%v", e)
		}
	}()
	//fmt.Printf("ClearItem+++")
	container := _item.GetContainer()
	if item, ok := container.(*itemInfo); ok && item != nil {
		if item.closed {
			return
		}
		item.closed = true
		<-self.chanTotal
		err := item.item.GetErr()
		item.item.SetContainer(nil)
		//fmt.Printf("ClearItem item:%p, err:%v, self:%v, chanTotal:%v, chanToNew:%v, chanIdle:%v", item, err, self.name, len(self.chanTotal), len(self.chanToNew), len(self.chanIdle))
		if err != errPoolClosed && err != errIdleFull {
			//fmt.Printf("ClearItem 1")
			select {
			case self.chanToNew <- 1:
			default:
			}
		}
	}
	//fmt.Printf("ClearItem---")
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
			fmt.Printf("panic:%v", e)
		}
	}()
	container := _item.GetContainer()
	/*item, ok := container.(*itemInfo)
	if !ok || nil == item {
		fmt.Printf("invalid poolItem")
		return
	}
	self.chanIdle <- item
	fmt.Printf("GiveBack item:%p", item)
	*/

	//fmt.Printf("doGiveBack+++")
	item, ok := container.(*itemInfo)
	if !ok || nil == item {
		fmt.Printf("invalid poolItem, self:%v", self.name)
		return
	}
	if item.closed {
		return
	}
	//fmt.Printf("doGiveBack 2")
	item.active = false
	item.idleTime = time.Now().Unix()
	select {
	case self.chanIdle <- item: //may send on closed channel
	default:
		self.closeItem(item, errIdleFull)
		fmt.Printf("errIdleFull item:%p, self:%v", item, self.name)
		return
	}
	unit++
	if unit >= 200 {
		unit = 0
		unitCount++
		fmt.Printf("doGiveBack unitCount:%v, self:%v", unitCount, self.name)
	}
	//fmt.Printf("doGiveBack item:%p", item)
}

// Close the pool.
func (self *Pool) Close() {
	fmt.Printf("Close Pool:%v", self.name)
	select {
	case <-self.chanClose:
		return
	default:
	}

	close(self.chanCheckMore)
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

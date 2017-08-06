package connpool

import (
	"errors"
	//. "serv/utils/mylog"
	"time"
)

type PoolItem interface {
	Close() error
	SetErr(error)
	GetErr() error
	SetContainer(PoolItem)
	GetContainer() PoolItem
}

type Creator interface {
	// Used to create a new item.
	// It will be called if there is no enough item
	// and there is still space to allocate new items.
	NewItem() (PoolItem, error)
	// This method will be called when the user
	// use Pool.Get() to get a item.
	// Every item returned from Get() will
	// be initialized with this method.
	//
	// item is the item will be returned.
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

func NewPool(name string, creator Creator, maxItemNum int, maxIdleNum int, idleTimeout int) *Pool {
	//GetLogger().InfofSkip(1, "NewPool, name:%v, maxItemNum:%v, maxIdleNum:%v, idleTimeout:%v", name, maxItemNum, maxIdleNum, idleTimeout)
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
	//GetLogger().InfofSkip(1, "NewPool:%v", pool.name)
	return pool
}

func (self *Pool) newItem() {
	defer func() {
		if e := recover(); e != nil {
			//GetLogger().Errorf("panic:%v, chanTotal closed?", e)
		}
	}()
	for {
		select {
		case <-self.chanClose:
			//GetLogger().Warnf("chanToNew closed")
			return
		case <-self.chanToNew:
		}
		self.chanTotal <- 1
		go func() {
			defer func() {
				if e := recover(); e != nil {
					//GetLogger().Errorf("panic:%v, chanIdle closed?", e)
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
				//GetLogger().Errorf("creator NewItem, self:%v, error:%v", self.name, err)
				return
			}
			itemInfo := newInfoItem(item)
			item.SetContainer(itemInfo)
			self.chanIdle <- itemInfo
			//GetLogger().Infof("newItem item:%p, self:%v, chanTotal:%v, chanToNew:%v, chanIdle:%v", itemInfo, self.name, len(self.chanTotal), len(self.chanToNew), len(self.chanIdle))
		}()
	}
}

func (self *Pool) checkMore() {
	defer func() {
		if e := recover(); e != nil {
			//GetLogger().Errorf("pool:%v closed, panic:%v", self.name, e)
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

func (self *Pool) SetGetTimeout(timeout int) {
	self.getTimeout = timeout
}

func (self *Pool) Get() (_item PoolItem, _err error) {
	/*item, ok := <-self.chanIdle
	if ok {
		//GetLogger().Debugf("Get item:%p", item)
		return item, nil
	}
	return nil, errors.New("closed")
	*/

	defer func() {
		if e := recover(); e != nil {
			//GetLogger().Errorf("pool closed, panic:%v", e)
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
				//GetLogger().Errorf("InitItem error, item:%v, self:%v, err:%v", item, self.name, err)
				self.closeItem(item, err)
				return nil, err
			}
			//GetLogger().Debugf("Get item:%p", item)
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
				//GetLogger().Errorf("InitItem error, item:%v, self:%v, err:%v", item, self.name, err)
				self.closeItem(item, err)
				return nil, err
			}
			//GetLogger().Debugf("Get item:%p", item)
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
	//GetLogger().Debugf("idleTime:%v, markTime:%v", item.idleTime, markTime)
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

func (self *Pool) ClearItem(item PoolItem) {
	go self.doClearItem(item)
}

func (self *Pool) doClearItem(_item PoolItem) {
	defer func() {
		if e := recover(); e != nil {
			//GetLogger().Errorf("panic:%v", e)
		}
	}()
	//GetLogger().Debugf("ClearItem+++")
	container := _item.GetContainer()
	if item, ok := container.(*itemInfo); ok && item != nil {
		if item.closed {
			return
		}
		item.closed = true
		<-self.chanTotal
		err := item.item.GetErr()
		item.item.SetContainer(nil)
		//GetLogger().Infof("ClearItem item:%p, err:%v, self:%v, chanTotal:%v, chanToNew:%v, chanIdle:%v", item, err, self.name, len(self.chanTotal), len(self.chanToNew), len(self.chanIdle))
		if err != errPoolClosed && err != errIdleFull {
			//GetLogger().Debugf("ClearItem 1")
			select {
			case self.chanToNew <- 1:
			default:
			}
		}
	}
	//GetLogger().Debugf("ClearItem---")
}

func (self *Pool) IsItemActive(_item PoolItem) bool {
	container := _item.GetContainer()
	if item, ok := container.(*itemInfo); ok && item != nil {
		return item.active
	}
	return false
}

func (self *Pool) GiveBack(item PoolItem) {
	go self.doGiveBack(item)
}

var unit uint64 = 0
var unitCount uint64 = 0

func (self *Pool) doGiveBack(_item PoolItem) {
	defer func() {
		if e := recover(); e != nil {
			//GetLogger().Errorf("panic:%v", e)
		}
	}()
	container := _item.GetContainer()
	/*item, ok := container.(*itemInfo)
	if !ok || nil == item {
		GetLogger().Errorf("invalid poolItem")
		return
	}
	self.chanIdle <- item
	//GetLogger().Debugf("GiveBack item:%p", item)
	*/

	//GetLogger().Debugf("doGiveBack+++")
	item, ok := container.(*itemInfo)
	if !ok || nil == item {
		//GetLogger().Errorf("invalid poolItem, self:%v", self.name)
		return
	}
	if item.closed {
		return
	}
	//GetLogger().Debugf("doGiveBack 2")
	item.active = false
	item.idleTime = time.Now().Unix()
	select {
	case self.chanIdle <- item: //may send on closed channel
	default:
		self.closeItem(item, errIdleFull)
		//GetLogger().Warnf("errIdleFull item:%p, self:%v", item, self.name)
		return
	}
	unit++
	if unit >= 200 {
		unit = 0
		unitCount++
		//GetLogger().Infof("doGiveBack unitCount:%v, self:%v", unitCount, self.name)
	}
	//GetLogger().Debugf("doGiveBack item:%p", item)
}

func (self *Pool) Close(args ...interface{}) {
	//GetLogger().InfofSkip(1, "Close Pool:%v", self.name)
	select {
	case <-self.chanClose:
		return
	default:
	}

	var forceClose bool = false
	if len(args) > 0 {
		var ok bool
		if forceClose, ok = args[0].(bool); !ok {
			forceClose = false
		}
	}
	close(self.chanCheckMore)
	close(self.chanToNew)
	close(self.chanTotal)
	close(self.chanClose)
	close(self.chanIdle)
	for item := range self.chanIdle {
		self.closeItem(item, errPoolClosed)
	}
	if forceClose {
	} else {
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

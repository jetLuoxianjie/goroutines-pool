package goroutinepool

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	GO_POOL_CAPACITY  = 500
	GO_POOL_IDLE_TIME = 60 * time.Second
)

var (
	once     sync.Once
	instance *StGoPoolMgr
)

type StGoPoolMgr struct {
	MCapacity    int32         // 当前协程池大小
	MReuseChan   chan func()   // 复用 chanel
	MNewPipeChan chan struct{} // 新建 channel
}

func GetInstance() *StGoPoolMgr {
	once.Do(func() {
		instance = &StGoPoolMgr{
		}
		instance.Init()
	})
	return instance
}

func (this *StGoPoolMgr) Init() {
	if this == nil {
		println("nil this")
		return
	}

	this.MCapacity = 0 // 统计当前协程池大小
	this.MReuseChan = make(chan func())
	this.MNewPipeChan = make(chan struct{}, GO_POOL_CAPACITY)

	go this.RunDaemonPipeline()
}

// 提交任务
func GO(goFunc func()) {
	goPoolMgr := GetInstance()
	if goPoolMgr == nil || goPoolMgr.MReuseChan == nil || goPoolMgr.MNewPipeChan == nil {
		println("nil")
		go goFunc()
		return
	}

	// 直接写select - case，如果都满足，会伪随机进入一个case，这里希望尽量复用协程池
	select {
	case goPoolMgr.MReuseChan <- goFunc:
		// 复用chan进入，说明有空闲的正在执行的pipeLine可以执行f
		return
	default:
		// 非阻塞，进入下一步，新启pipeline
	}

	select {
	case goPoolMgr.MNewPipeChan <- struct{}{}:
		// 新chan进入，说明可以在容量范围内，开辟一个pipeLine
		go goPoolMgr.RunPipeLine(goFunc)
	default:
		// 如果都不行，说明流水线都在运行，且数量超过上限了，则直接go goFunc，不再走协程池
		println("[GoPoolLog] Pool Cap reach Max")
		go goFunc()
	}
}

// 会有一个常驻的goroutine，用来解决第一个func进入MReuseChan后，无法立即处理的问题
func (this *StGoPoolMgr) RunDaemonPipeline() {
	if this == nil || this.MReuseChan == nil || this.MNewPipeChan == nil {
		println("nil")
		return
	}

	// 每次执行，更新定时器
	defer func() {
		println("[GoPoolLog] Recover Daemon Pipe ???")
	}()

	var goFunc func()
	for {
		// 阻塞直到有新的goFunc传入
		select {
		case goFunc = <-this.MReuseChan:
		}
		if goFunc == nil {
			println( "[GoPoolLog] nil goFunc?")
		} else {
			// 执行任务
			println("[GoPoolLog] Daemon Reuse Pipe, cur capacity[%d]", this.MCapacity)
			goFunc()
		}
	}
}

func (this *StGoPoolMgr) RunPipeLine(goFunc func()) {
	if this == nil || this.MReuseChan == nil || this.MNewPipeChan == nil {
		println(false, "nil")
		go goFunc()
		return
	}

	// 每次执行，更新定时器
	atomic.AddInt32(&this.MCapacity, 1)
	println("[GoPoolLog] 1. New Pipe, cur capacity[%d]", this.MCapacity)
	timer := time.NewTimer(GO_POOL_IDLE_TIME)

	defer func() {
		timer.Stop()
		atomic.AddInt32(&this.MCapacity, -1)
		// 超时回收空闲任务
		select {
		case <-this.MNewPipeChan:
		default:
			// 理论上不该发生，看看是为啥
			fmt.Printf( "[GoPoolLog] Recover Pipe Failed???\n")
		}
	}()

	for {
		// 执行任务
		goFunc()
		select {
		case <-timer.C:
			// idle超时，则离开
			fmt.Printf("[GoPoolLog] RunPipeLine TimeOut, Pipe recover, cur capacity[%d]\n", this.MCapacity)
			return
		case goFunc = <-this.MReuseChan:
			if goFunc == nil {
				fmt.Printf( "[GoPoolLog] nil goFunc?\n")
				return
			}
			fmt.Printf("[GoPoolLog] 2. Reuse Pipe, cur capacity[%d]\n", this.MCapacity)
			// 重置超时时间
			timer.Reset(GO_POOL_IDLE_TIME)
		}
	}
}

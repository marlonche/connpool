package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"sync"

	"net"
	"net/http"
	_ "net/http/pprof"
	"time"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile `file`")
var flagAsServer = flag.Bool("asServer", false, "run as server demo")
var flagAddrServer = flag.String("addr", "", "server address")
var flagGetCount = flag.Int("getCount", 5000, "get count")
var flagLoopCount = flag.Int("loopCount", 4, "loop count")
var flagLoopInterval = flag.Int("loopInterval", 15, "loop interval")
var flagRoutineLimit = flag.Int("routineLimit", 80000, "courrent goroutines")
var flagMaxTotalNum = flag.Int("maxTotalNum", 10, "maximum total number of active and idle items")
var flagMaxIdleNum = flag.Int("maxIdleNum", 5, "maximum number of idle items")
var flagIdleTimeout = flag.Int("idleTimeout", 60, "idle timeout in second")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	asServer := *flagAsServer
	addr := *flagAddrServer
	pprofAddr := ":6060"
	if asServer {
		pprofAddr = ":6061"
	}
	go func() {
		fmt.Println(http.ListenAndServe(pprofAddr, nil))
	}()

	if asServer {
		runAsServer(addr)
	} else {
		runAsClient(addr)
	}
}

func runAsClient(addr string) {
	if len(addr) == 0 {
		addr = "127.0.0.1:9999"
	}
	chanRoutineLimit := make(chan struct{}, *flagRoutineLimit)
	var wg sync.WaitGroup
	pool := NewStreamPool("pool-name", addr, *flagMaxTotalNum, *flagMaxIdleNum, *flagIdleTimeout)
	for j := 0; j < *flagLoopCount; j++ {
		for i := 0; i < *flagGetCount; i++ {
			chanRoutineLimit <- struct{}{}
			wg.Add(1)
			go func() {
				defer func() {
					wg.Done()
					<-chanRoutineLimit
				}()
				conn, err := pool.Get()
				if err != nil {
					fmt.Printf("pool.Get() error: %v\n", err)
					return
				}
				defer conn.Close()
				content := fmt.Sprintf("Hello, my id is %v\n", time.Now().Nanosecond())
				_, err = conn.Write([]byte(content))
				if err != nil {
					fmt.Printf("conn write error: %v\n", err)
					conn.SetErr(err)
				}
			}()
		}
		time.Sleep(time.Second * time.Duration(*flagLoopInterval))
	}
	wg.Wait()
}

func runAsServer(addr string) {
	if len(addr) == 0 {
		addr = ":9999"
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("listen error: %v\n", err)
		return
	}
	fmt.Printf("listen address:%v\n", l.Addr())
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Printf("Accept error: %v\n", err)
			return
		}
		fmt.Printf("accept conn:%v\n", conn.RemoteAddr())
		go func() {
			n, err := io.Copy(conn, conn)
			fmt.Printf("n:%v, err:%v\n", n, err)
		}()
	}
}

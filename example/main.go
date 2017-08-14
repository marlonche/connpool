package main

import (
	"bufio"
	"flag"
	"fmt"

	"net"
	"net/http"
	_ "net/http/pprof"
	"time"
)

var flagAsServer = flag.Bool("asServer", false, "run as server demo")
var flagAddrServer = flag.String("addr", "", "server address")
var flagGetCount = flag.Int("getCount", 5000, "get count")
var flagLoopCount = flag.Int("loopCount", 4, "loop count")
var flagLoopInterval = flag.Int("loopInterval", 15, "loop interval")
var flagMaxTotalNum = flag.Int("maxTotalNum", 10, "maximum total number of active and idle items")
var flagMaxIdleNum = flag.Int("maxIdleNum", 5, "maximum number of idle items")
var flagIdleTimeout = flag.Int("idleTimeout", 60, "idle timeout in second")

func main() {
	flag.Parse()
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
	pool := NewStreamPool("pool-name", addr, *flagMaxTotalNum, *flagMaxIdleNum, *flagIdleTimeout)
	for j := 0; j < *flagLoopCount; j++ {
		for i := 0; i < *flagGetCount; i++ {
			go func() {
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
	time.Sleep(time.Hour)
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
			r := bufio.NewReader(conn)
			for {
				s, err := r.ReadString('\n')
				if err != nil {
					fmt.Printf("ReadString err: %v\n", err)
					return
				}
				if _, err = conn.Write([]byte(s)); err != nil {
					fmt.Printf("Write error: %v\n", err)
					return
				}
			}
		}()
	}
}

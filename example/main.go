package main

import (
	"bufio"
	"flag"
	"fmt"

	"net"
	"time"
)

var flagAsServer = flag.Bool("asServer", false, "run as server demo")
var flagAddrServer = flag.String("addr", "", "server address")

func main() {
	flag.Parse()
	asServer := *flagAsServer
	addr := *flagAddrServer
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
	pool := NewStreamPool("pool-name", addr, 10, 5, 60)
	for i := 0; i < 5000; i++ {
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
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Printf("Accept error: %v\n", err)
			return
		}
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

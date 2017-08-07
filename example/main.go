package main

import (
	"bufio"
	"flag"
	"fmt"

	"net"
	"time"
)

var flagAsServer = flag.Bool("asServer", false, "run as server demo")

func main() {
	flag.Parse()
	asServer := *flagAsServer
	if asServer {
		runAsServer()
	} else {
		runAsClient()
	}
}

func runAsClient() {
	pool := NewStreamPool("pool-name", "127.0.0.1:9999", 10, 5, 60)
	for i := 0; i < 5000; i++ {
		go func() {
			conn, err := pool.Get()
			if err != nil {
				fmt.Printf("pool.Get() error: %v", err)
				return
			}
			defer conn.Close()
			content := fmt.Sprintf("Hello, my id is %v\n", time.Now().Nanosecond())
			_, err = conn.Write([]byte(content))
			if err != nil {
				fmt.Printf("conn write error: %v", err)
				conn.SetErr(err)
			}
		}()
	}
	time.Sleep(time.Hour)
}

func runAsServer() {
	l, err := net.Listen("tcp", ":9999")
	if err != nil {
		fmt.Printf("listen error: %v", err)
		return
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Printf("Accept error: %v", err)
			return
		}
		go func() {
			r := bufio.NewReader(conn)
			for {
				s, err := r.ReadString('\n')
				if err != nil {
					fmt.Printf("ReadString err: %v", err)
					return
				}
				if _, err = conn.Write([]byte(s)); err != nil {
					fmt.Printf("Write error: %v", err)
					return
				}
			}
		}()
	}
}

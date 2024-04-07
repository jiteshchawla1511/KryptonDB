package server

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	lsmtree "github.com/jiteshchawla1511/KryptonDB/LSM_Tree"
	dbengine "github.com/jiteshchawla1511/KryptonDB/dbEngine"
	"github.com/jiteshchawla1511/KryptonDB/wal"
)

const (
	DefaultTCPPort       = "8080"
	DefaultUDPPort       = "1053"
	DefaultUDPBufferSize = 1024
	DefaultHost          = "localhost"
)

type Server struct {
	Port          string
	Host          string
	UDPPort       string
	UDPBufferSize int
	DBEngine      *dbengine.DBEngine
}

func (s *Server) Start() {
	listen, err := net.Listen("tcp", fmt.Sprintf("%s:%s", s.Host, s.Port))
	if err != nil {
		fmt.Println("Error Listening")
		return
	}
	defer listen.Close()

	udpServer, err := net.ListenPacket("udp", fmt.Sprintf("%s:%s", s.Host, s.UDPPort))
	if err != nil {
		fmt.Println("Error Listening UDP Port")
		return
	}
	defer udpServer.Close()

	dataLoadSignal := make(chan bool, 1)
	startPersistCycle := make(chan bool, 1)

	go func() {
		fmt.Println("Loading data from disk")

		err := s.DBEngine.LoadFromDisk(s.DBEngine.Lsmtree, s.DBEngine.WAL)

		if err != nil {
			fmt.Printf("Error loading data from disk")
			panic(err)
		}

		fmt.Println("Data loaded from disk")

		dataLoadSignal <- true
		startPersistCycle <- true
	}()

	<-dataLoadSignal

	go s.DBEngine.Store.PersistToDisk(s.DBEngine.WAL, startPersistCycle)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("Shutting down server")
		err := s.DBEngine.WAL.Persist()

		if err != nil {
			fmt.Printf("Error persisting WAL\n")
		}

		os.Exit(0)
	}()

	go func() {
		for {
			conn, err := listen.Accept()
			if err != nil {
				fmt.Printf("Error accepting (TCP)")
				continue
			}

			go handleConnection(conn, s.DBEngine.Lsmtree, s.DBEngine.WAL)
		}
	}()

	go func() {
		buf := make([]byte, s.UDPBufferSize)
		for {
			n, addr, err := udpServer.ReadFrom(buf)
			if err != nil {
				fmt.Printf("Error reading UDP packet")
				continue
			}

			go handleUDPPacket(udpServer, buf[:n], addr, s.DBEngine.Lsmtree, s.DBEngine.WAL)
		}
	}()

	select {}

}

func handleConnection(conn net.Conn, ltree *lsmtree.LSMTree, wal *wal.WAL) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	writer := bufio.NewWriter(conn)

	for scanner.Scan() {
		text := scanner.Text()

		cmd := strings.Split(text, " ")

		if len(cmd) == 0 {
			writer.WriteString("Invalid command\n")
		}

		switch cmd[0] {
		case "PUT":
			if len(cmd) != 3 {
				writer.WriteString("Invalid command\n")
				writer.Flush()
				continue
			}

			err := wal.Write([]byte("+"), []byte(cmd[1]), []byte(cmd[2]))

			if err != nil {
				writer.WriteString("Error writing to WAL\n")
				writer.Flush()
				continue
			}

			ltree.Put(cmd[1], cmd[2])
			writer.WriteString("OK\n")
			writer.Flush()
		case "GET":

			err := wal.Persist()

			if err != nil {
				writer.WriteString("Error persisting WAL\n")
				writer.Flush()
				continue
			}

			val, exist := ltree.Get(cmd[1])
			if !exist {
				writer.WriteString("Data not found\n")
				writer.Flush()
			} else {
				writer.WriteString(val + "\n")
				writer.Flush()
			}
		case "DEL":

			err := wal.Write([]byte("-"), []byte(cmd[1]))

			if err != nil {
				writer.WriteString("Error writing to WAL\n")
				writer.Flush()
				continue
			}

			ltree.Del(cmd[1])
			writer.WriteString("OK\n")
			writer.Flush()
		default:
			writer.WriteString("Invalid command\n")
			writer.Flush()
		}

	}

}

func handleUDPPacket(udpConn net.PacketConn, packet []byte, addr net.Addr, ltree *lsmtree.LSMTree, wal *wal.WAL) {

	response := ""

	request := string(packet)

	cmd := strings.Split(request, " ")

	if len(cmd) == 0 {
		response = "Invalid command"
	} else {
		switch cmd[0] {
		case "GET":
			if len(cmd) != 2 {
				response = "Invalid command"
				break
			}

			err := wal.Persist()

			if err != nil {
				response = "Error persisting WAL"
				break
			}

			cmd[1] = strings.Trim(cmd[1], "\n")

			val, exist := ltree.Get(cmd[1])

			if !exist {
				response = "Data not found"
			} else {
				response = val
			}
		default:
			response = "Invalid command"
		}
	}

	responseBytes := []byte(response)

	_, err := udpConn.WriteTo(responseBytes, addr)
	if err != nil {
		fmt.Println("Error sending UDP response")
	}
}

//go:build darwin

package server

import (
	"log"
	"net"
	"syscall"

	"github.com/dicedb/dice/config"
	"github.com/dicedb/dice/core"
)

var con_clients int = 0

func RunAsyncTCPServer() error {
	log.Println("starting an asynchronous TCP server on", config.Host, config.Port)

	max_clients := 20000

	// Create a socket
	serverFD, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		return err
	}
	defer syscall.Close(serverFD)

	// Set the Socket to operate in non-blocking mode
	if err = syscall.SetNonblock(serverFD, true); err != nil {
		return err
	}

	// Bind the IP and the port
	ip4 := net.ParseIP(config.Host)
	if err = syscall.Bind(serverFD, &syscall.SockaddrInet4{
		Port: config.Port,
		Addr: [4]byte{ip4[0], ip4[1], ip4[2], ip4[3]},
	}); err != nil {
		return err
	}

	// Start listening
	if err = syscall.Listen(serverFD, max_clients); err != nil {
		return err
	}

	// AsyncIO starts here — using KQUEUE (macOS equivalent of epoll)

	// Create a kqueue instance
	kqueueFD, err := syscall.Kqueue()
	if err != nil {
		log.Fatal(err)
	}
	defer syscall.Close(kqueueFD)

	// Register the server socket for READ events
	// This is like epoll's EPOLL_CTL_ADD with EPOLLIN
	changeEvent := syscall.Kevent_t{
		Ident:  uint64(serverFD),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_ADD | syscall.EV_ENABLE,
		Fflags: 0,
		Data:   0,
		Udata:  nil,
	}

	changeList := []syscall.Kevent_t{changeEvent}
	if _, err := syscall.Kevent(kqueueFD, changeList, nil, nil); err != nil {
		return err
	}

	// Event buffer to hold events returned by kqueue
	events := make([]syscall.Kevent_t, max_clients)

	for {
		// Wait for events — like EpollWait
		nevents, err := syscall.Kevent(kqueueFD, nil, events, nil)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			log.Println("kevent wait error:", err)
			continue
		}

		for i := 0; i < nevents; i++ {
			fd := int(events[i].Ident)

			// If the server socket is ready — a new client is connecting
			if fd == serverFD {
				clientFD, _, err := syscall.Accept(serverFD)
				if err != nil {
					log.Println("accept error:", err)
					continue
				}

				con_clients++
				syscall.SetNonblock(clientFD, true)

				// Register the new client for READ events
				clientEvent := syscall.Kevent_t{
					Ident:  uint64(clientFD),
					Filter: syscall.EVFILT_READ,
					Flags:  syscall.EV_ADD | syscall.EV_ENABLE,
					Fflags: 0,
					Data:   0,
					Udata:  nil,
				}
				if _, err := syscall.Kevent(kqueueFD, []syscall.Kevent_t{clientEvent}, nil, nil); err != nil {
					log.Fatal(err)
				}

				log.Println("new client connected, concurrent clients:", con_clients)
			} else {
				// A client socket has data to read
				comm := core.FDComm{Fd: fd}
				cmd, err := readCommand(comm)
				if err != nil {
					syscall.Close(fd)
					con_clients--
					log.Println("client disconnected, concurrent clients:", con_clients)
					continue
				}
				respond(cmd, comm)
			}
		}
	}
}
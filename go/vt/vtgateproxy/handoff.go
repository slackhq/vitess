package vtgateproxy

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// handoff implements a no-downtime handoff of a TCP listener from one running
// process to another. It can be used for no-downtime deploys of HTTP servers
// on a single host/port.

// ListenForHandoff opens a unix domain socket and listens for handoff
// requests. When a handoff request is received, the underlying file
// descriptor of `listener` is handed off over the socket.
//
// If an error occurs while opening the unix domain
// socket, or during handoff, it will be logged and the listener will resume
// listening. Otherwise, Listen will return nil when the handoff is complete.
//
// Callers should drain any servers
// connected to the net.Listener, and in-flight requests should be resolved
// before shutting down.
func ListenForHandoff(socketPath string, listener net.Listener) error {
	// Clean up any leftover sockets that might have gotten left from previous
	// processes.
	os.Remove(socketPath)

	unixListener, err := net.Listen("unix", socketPath)
	if err != nil {
		return err
	}
	defer func() {
		unixListener.Close()
		os.Remove(socketPath)
	}()

	for {
		err := listen(unixListener, listener)
		if err != nil {
			slog.Error("handoff socket error", "error", err)
			continue
		}

		return nil
	}
}

var magicPacket = "handoff"

func listen(unixListener, listener net.Listener) error {
	conn, err := unixListener.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()
	err = conn.SetDeadline(time.Now().Add(1 * time.Second))
	if err != nil {
		return err
	}

	b := make([]byte, len(magicPacket))
	n, err := conn.Read(b)
	if err != nil {
		return err
	}
	if string(b[:n]) != magicPacket {
		return errors.New("bad magic packet")
	}

	return handoff(conn, listener)
}

func handoff(conn net.Conn, listener net.Listener) error {
	unixFD, err := getFD(conn.(*net.UnixConn))
	if err != nil {
		return err
	}

	tcpListener := listener.(*net.TCPListener)

	tcpFd, err := getFD(tcpListener)
	if err != nil {
		return err
	}

	rights := unix.UnixRights(tcpFd)
	err = unix.Sendmsg(unixFD, nil, rights, nil, 0)
	if err != nil {
		return err
	}

	return nil
}

// RequestHandoff checks for the presence of a unix domain socket at
// `socketPath` and opens a connection. The server side of the socket will
// immediately send a file descriptor of a TCP socket over the unix domain
// socket. This file descriptor is converted into a net.Listener and returned
// to the caller for immediate use.
//
// During the time between socket handoff and startup of the new server,
// requests to the socket will block. Requests will only fail if the client
// timeout is shorter than the duration of the handoff period.
//
// If nothing is listening on the other end of the unix domain socket,
// ErrNoHandoff is returned. Clients should check for this condition, and dial
// the TCP socket themselves.
func RequestHandoff(socketPath string) (net.Listener, error) {
	if socketPath == "" {
		return nil, ErrNoHandoff
	}
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNoHandoff, err)
	}
	defer conn.Close()
	err = conn.SetDeadline(time.Now().Add(1 * time.Second))
	if err != nil {
		return nil, err
	}

	_, err = conn.Write([]byte(magicPacket))
	if err != nil {
		return nil, fmt.Errorf("%w: failed to send magic packet", err)
	}

	f, err := (conn.(*net.UnixConn)).File()
	if err != nil {
		return nil, fmt.Errorf("%w: fd not read", err)
	}
	defer f.Close()

	b := make([]byte, unix.CmsgSpace(4))
	//nolint:dogsled
	_, _, _, _, err = unix.Recvmsg(int(f.Fd()), nil, b, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: msg not received", err)
	}

	cmsgs, err := unix.ParseSocketControlMessage(b)
	if err != nil {
		return nil, fmt.Errorf("%w: control msg not parsed", err)
	}
	fds, err := unix.ParseUnixRights(&cmsgs[0])
	if err != nil {
		return nil, fmt.Errorf("%w: invalid unix rights", err)
	}
	fd := fds[0]

	listenerFD := os.NewFile(uintptr(fd), "listener")
	defer f.Close()

	l, err := net.FileListener(listenerFD)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to acquire new fd", err)
	}

	return l, nil
}

// ErrNoHandoff indicates that no handoff was performed.
var ErrNoHandoff = errors.New("no handoff")

func getFD(conn syscall.Conn) (fd int, err error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return -1, err
	}

	err = raw.Control(func(ptr uintptr) {
		fd = int(ptr)
	})
	return fd, err
}

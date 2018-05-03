package logstash

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// Logstash is the basic struct
type Logstash struct {
	Hostname   string
	Port       int
	Connection *net.TCPConn
	//Timeout in milliseconds
	Timeout int
}

// New generates a logstash sender from a host:port format
func New(host string, timeout int) (ls *Logstash, err error) {

	lshost, lsportstring, err := net.SplitHostPort(host)
	if err != nil {
		return ls, errors.Wrap(err, "net-splithost")
	}
	lsport, err := strconv.Atoi(lsportstring)
	if err != nil {
		return ls, errors.Wrap(err, "logstash port isn't numeric")
	}

	// temporary at 3 minues.  Or I can build the connection after I get the first row back
	ls = NewHostPort(lshost, lsport, 180000)

	return ls, nil
}

// NewHostPort makes a logstash sender from a host name and port
func NewHostPort(hostname string, port int, timeout int) *Logstash {
	l := Logstash{}
	l.Hostname = hostname
	l.Port = port
	l.Connection = nil
	l.Timeout = timeout
	return &l
}

// Dump prints the contents of the Logstash structure
func (l *Logstash) Dump() {
	fmt.Println("Hostname:   ", l.Hostname)
	fmt.Println("Port:       ", l.Port)
	fmt.Println("Connection: ", l.Connection)
	fmt.Println("Timeout:    ", l.Timeout)
}

// SetTimeouts sets the timeout values
func (l *Logstash) SetTimeouts() {
	deadline := time.Now().Add(time.Duration(l.Timeout) * time.Millisecond)
	l.Connection.SetDeadline(deadline)
	l.Connection.SetWriteDeadline(deadline)
	l.Connection.SetReadDeadline(deadline)
}

// Connect to the host
func (l *Logstash) Connect() (*net.TCPConn, error) {
	var connection *net.TCPConn
	service := fmt.Sprintf("%s:%d", l.Hostname, l.Port)
	addr, err := net.ResolveTCPAddr("tcp", service)
	if err != nil {
		return connection, err
	}
	connection, err = net.DialTCP("tcp", nil, addr)
	if err != nil {
		return connection, err
	}
	if connection != nil {
		l.Connection = connection
		l.Connection.SetLinger(0) // default -1
		l.Connection.SetNoDelay(true)
		l.Connection.SetKeepAlive(true)
		l.Connection.SetKeepAlivePeriod(time.Duration(5) * time.Second)
		l.SetTimeouts()
	}
	return connection, err
}

// Writeln send a message to the host
func (l *Logstash) Writeln(message string) error {
	var err = errors.New("tcp connection is nil")
	message = fmt.Sprintf("%s\n", message)
	if l.Connection != nil {
		_, err = l.Connection.Write([]byte(message))
		if err != nil {
			if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
				l.Connection.Close()
				l.Connection = nil
				if err != nil {
					return err
				}
			} else {
				l.Connection.Close()
				l.Connection = nil
				return err
			}
		} else {
			// Successful write! Let's extend the timeoul.
			l.SetTimeouts()
			return nil
		}
	}
	return err
}

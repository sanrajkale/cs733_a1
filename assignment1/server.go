// server.go
// server.go
package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Tuple struct {
	expt  time.Time
	val   string
	ver   int64
	nbyte int64
}

type Hash struct {
	sync.RWMutex
	keyval map[string]Tuple
}

func main() {

	var hashtab Hash
	hashtab.keyval = make(map[string]Tuple)
	listener, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Println("Error Listen :", err.Error())
		return
	}
	//go expKeyRemover(hashtab) // routine to remove expired key value pair
	for {
		connection, err := listener.Accept() // this blocks until connection or error
		if err != nil {
			log.Println("Error Accept:", err.Error())
			continue
		}
		go clientHandler(connection, hashtab) // a goroutine handles conn so that the loop can accept other connections
	}
}

func expKeyRemover(hashtab Hash) {
	//func ParseDuration(s string) (Duration, error)
	dur, _ := time.ParseDuration("3s")

	for {
		time.Sleep(dur)
		var keys []string
		hashtab.RLock()
		for k := range hashtab.keyval {
			t1 := hashtab.keyval[k].expt
			if t1.Before(time.Now()) && !t1.IsZero() {
				keys = append(keys, k)
			}
		}
		hashtab.RUnlock()
		for k := range keys {
			hashtab.Lock()
			tmptuple, ok := hashtab.keyval[keys[k]]
			if ok && tmptuple.expt.Before(time.Now()) {
				delete(hashtab.keyval, keys[k])
			}
			hashtab.Unlock()
		}

	}
}

//This routine serves one client.
func clientHandler(conn net.Conn, hashtab Hash) {

	for {
		buf := make([]byte, 1024) // buffer to read first line of command for operation
		// Read the incoming connection into the buffer.
		_, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading:", err.Error())
			break
		}

		n := bytes.IndexByte(buf, '\r')
		cmd := string(buf[:n])

		cmd_argu := strings.Fields(cmd)

		switch {
		case cmd_argu[0] == "set":
			hashtab.set(conn, cmd_argu) // here DOTO: use chan to run routine as go routine
		case cmd_argu[0] == "get":
			hashtab.get(conn, cmd_argu)
		case cmd_argu[0] == "delete":
			hashtab.del(conn, cmd_argu)
		case cmd_argu[0] == "getm":
			hashtab.getm(conn, cmd_argu)
		case cmd_argu[0] == "cas":
			hashtab.cas(conn, cmd_argu)
		default:
			conn.Write([]byte("ERRCMDERR\r\n"))
			continue
		}
	}
}

func (hashtab Hash) set(conn net.Conn, cmd_argu []string) {

	// cmd_argu = set <key> <exptime> <numbytes> [noreply]\r\n

	if len(cmd_argu) < 4 || len(cmd_argu) > 5 {
		conn.Write([]byte("ERRCMDERR\r\n"))
		return
	}
	noreply := false

	if len(cmd_argu) == 5 {
		if cmd_argu[4] != "noreply" {
			noreply = true
		}
	}

	key := cmd_argu[1]
	exptime := cmd_argu[2] + "s"

	dur, durerr := time.ParseDuration(exptime)
	if durerr != nil {
		conn.Write([]byte("ERRCMDERR\r\n"))
		return
	}

	numbytes, atoierr := strconv.ParseInt(cmd_argu[3], 10, 64)
	if atoierr != nil {
		conn.Write([]byte("ERRCMDERR\r\n"))
		return
	}

	log.Println(key)
	log.Println(exptime)
	log.Println(numbytes)

	buf := make([]byte, numbytes+2)

	// Read the incoming connection into the buffer.
	log.Println(numbytes)
	_, err := conn.Read(buf)
	log.Println(numbytes)
	if err != nil {
		conn.Write([]byte("ERR_INTERNAL\r\n"))
		return
	}
	log.Println(buf)
	// Builds the message.
	n := int64(bytes.IndexByte(buf, '\r'))
	if n > numbytes {
		// Size of data more than expected
		conn.Write([]byte("ERRCMDERR\r\n"))
		return
	}
	value := string(buf[:n])

	tmptuple := Tuple{}

	hashtab.Lock()
	log.Println(key)
	if valtuple, ok := hashtab.keyval[key]; ok {
		tmptuple = valtuple
		if dur.Seconds() > 0 {
			tmptuple.expt = time.Now().Add(dur)
		} else {
			tmptuple.expt = time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)
		}
		tmptuple.val = value
		tmptuple.nbyte = int64(len(value))
		tmptuple.ver++

	} else {
		if dur.Seconds() > 0 {
			tmptuple.expt = time.Now().Add(dur)
		} else {
			tmptuple.expt = time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)
		}
		tmptuple.val = value
		tmptuple.nbyte = int64(len(value))
		tmptuple.ver = 1
	}

	hashtab.keyval[key] = tmptuple

	if noreply == false {
		message := "OK " + strconv.FormatInt(tmptuple.ver, 10) + "\r\n"
		conn.Write([]byte(message))
	}

	hashtab.Unlock()
	log.Println(key)

}

func (hashtab Hash) get(conn net.Conn, cmd_argu []string) {

	// get <key>\r\n

	if len(cmd_argu) != 2 {
		conn.Write([]byte("ERRCMDERR\r\n"))
		return
	}

	hashtab.RLock()
	tmptuple, ok := hashtab.keyval[cmd_argu[1]]
	if ok && tmptuple.expt.After(time.Now()) {
		message := "VALUE " + strconv.FormatInt(tmptuple.nbyte, 10) + "\r\n"
		conn.Write([]byte(message))
		valmsg := tmptuple.val + "\r\n"
		conn.Write([]byte(valmsg))
	} else {
		conn.Write([]byte("ERRNOTFOUND\r\n"))
	}
	hashtab.RUnlock()

}

func (hashtab Hash) del(conn net.Conn, cmd_argu []string) {

	//delete <key>\r\n
	//DELETED\r\n

	if len(cmd_argu) != 2 {
		conn.Write([]byte("ERRCMDERR\r\n"))
		return
	}

	hashtab.Lock()
	if _, ok := hashtab.keyval[cmd_argu[1]]; ok {
		delete(hashtab.keyval, cmd_argu[1])
		message := "DELETED\r\n"
		conn.Write([]byte(message))
	} else {
		conn.Write([]byte("ERRNOTFOUND\r\n"))
	}
	hashtab.Unlock()

}

func (hashtab Hash) getm(conn net.Conn, cmd_argu []string) {

	//VALUE <version> <exptime> <numbytes>\r\n
	//<value bytes>\r\n

	if len(cmd_argu) != 2 {
		conn.Write([]byte("ERRCMDERR\r\n"))
		return
	}

	hashtab.RLock()
	if tmptuple, ok := hashtab.keyval[cmd_argu[1]]; ok {
		var message string
		if tmptuple.expt.IsZero() {
			message = "VALUE " + strconv.FormatInt(tmptuple.ver, 10) + " 0 " + strconv.FormatInt(tmptuple.nbyte, 10) + "\r\n"
		} else {
			message = "VALUE " + strconv.FormatInt(tmptuple.ver, 10) + " " + strconv.FormatFloat(tmptuple.expt.Sub(time.Now()).Seconds(), 'f', 2, 64) + " " + strconv.FormatInt(tmptuple.nbyte, 10) + "\r\n"
		}
		conn.Write([]byte(message))
		valmsg := tmptuple.val + "\r\n"
		conn.Write([]byte(valmsg))
		//	fmt.Println(tmptuple.val)
	} else {
		conn.Write([]byte("ERRNOTFOUND\r\n"))
	}
	hashtab.RUnlock()

}
func (hashtab Hash) cas(conn net.Conn, cmd_argu []string) {
	// cas <key> <exptime> <version> <numbytes> [noreply]\r\n
	//<value bytes>\r\n
	//The server responds with the new version if successful (or one of the errors described late)
	//  OK <version>\r\n

	if len(cmd_argu) < 5 || len(cmd_argu) > 6 {
		conn.Write([]byte("ERRCMDERR\r\n"))
		return
	}

	noreply := false

	if len(cmd_argu) == 6 {
		if cmd_argu[5] != "noreply" {
			noreply = true
		}
	}

	key := cmd_argu[1]
	exptime := cmd_argu[2] + "s"
	dur, durerr := time.ParseDuration(exptime)
	if durerr != nil {
		conn.Write([]byte("ERRCMDERR\r\n"))
		return
	}

	version, vererr := strconv.ParseInt(cmd_argu[3], 10, 64)
	if vererr != nil {
		conn.Write([]byte("ERRCMDERR\r\n"))
		return
	}

	numbytes, numbyteserr := strconv.ParseInt(cmd_argu[4], 10, 64)
	if numbyteserr != nil {
		conn.Write([]byte("ERRCMDERR\r\n"))
		return
	}

	//fmt.Println(key)
	//fmt.Println(exptime)
	//fmt.Println(version)
	//fmt.Println(numbytes)

	buf := make([]byte, numbytes+2)

	// Read the incoming connection into the buffer.
	_, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading:", err.Error())
		return
	}

	// Builds the message.
	n := int64(bytes.IndexByte(buf, '\r'))
	if n > numbytes {
		// Size of data more than expected
		conn.Write([]byte("“ERRCMDERR\r\n"))
		return
	}

	value := string(buf[:n])

	tmptuple := Tuple{}

	hashtab.Lock()
	if valtuple, ok := hashtab.keyval[key]; ok {
		if version == valtuple.ver {
			tmptuple = valtuple
			if dur.Seconds() > 0 {
				tmptuple.expt = time.Now().Add(dur)
			} else {
				tmptuple.expt = time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)

			}
			tmptuple.val = value
			tmptuple.ver++
			tmptuple.nbyte = numbytes
			hashtab.keyval[key] = tmptuple
			if noreply == false {
				message := "OK " + strconv.FormatInt(tmptuple.ver, 10) + "\r\n"
				conn.Write([]byte(message))
			}
		} else {
			conn.Write([]byte("ERR_VERSION \r\n"))
		}
	} else {
		conn.Write([]byte("ERRNOTFOUND \r\n"))
	}
	hashtab.Unlock()
}

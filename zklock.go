package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
)

const (
	READ  = "read"
	WRITE = "write"
)

type nodeSlice []string

func (s nodeSlice) Len() int      { return len(s) }
func (s nodeSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s nodeSlice) Less(i, j int) bool {
	num_i := strings.Split(s[i], "-")[1]
	num_j := strings.Split(s[j], "-")[1]
	return num_i < num_j
}

type ZKLock struct {
	conn      *zk.Conn
	lockName  string
	ReadLock  string
	WriteLock string
}

func (zl *ZKLock) Init(url []string, lockname string) error {
	zl.lockName = lockname
	var err error
	zl.conn, _, err = zk.Connect(url, time.Second*5)
	if err != nil {
		panic(err)
	}
	fmt.Println("check if root dir exist: " + lockname)
	isExist, _, err := zl.conn.Exists(zl.lockName)
	if err != nil {
		panic(err)
	}
	if !isExist {
		fmt.Println("Dir not exist: " + lockname)
		if _, err := zl.conn.Create(zl.lockName, nil, 0, zk.WorldACL(zk.PermAll)); err != nil {
			return err
		}
	}
	return nil
}

func (zl *ZKLock) lockRead() error {
	wg := sync.WaitGroup{}
	var err error
	thisLockNodeBuilder := zl.lockName + "/" + string(READ) + "-"
	zl.ReadLock, err = zl.conn.Create(thisLockNodeBuilder, nil, zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil {
		panic(err)
	}
	wg.Add(1)
	// find the write lock before this readlock
	tmp_nodes, _, err := zl.conn.Children(zl.lockName)
	if err != nil {
		panic(err)
	}
	sort.Sort(nodeSlice(tmp_nodes))
	fmt.Println("Read nodes", tmp_nodes)
	var tmp_index = 0
	// do we have the less index write node?
	for i := len(tmp_nodes) - 1; i >= 0; i-- {
		if zl.ReadLock == zl.lockName+"/"+tmp_nodes[i] {
			tmp_index = i
		} else if i < tmp_index && strings.Split(tmp_nodes[i], "-")[0] == string(WRITE) {
			// find a write lock before this read lock
			// listen this write lock and block the read lock
			fmt.Println("read gotcha")
			_, _, event, err := zl.conn.ExistsW(zl.lockName + "/" + tmp_nodes[i])
			if err != nil {
				panic(err)
			}
			go func() {
				defer wg.Done()
				e := <-event
				fmt.Println("path: ", e.Path)
			}()
			wg.Wait()
			break
		}
	}
	return nil
}

func (zl *ZKLock) unlockRead() error {
	if zl.ReadLock != "" {
		// need version of the lock
		_, state, err := zl.conn.Get(zl.ReadLock)
		if err != nil {
			panic(err)
		}
		err = zl.conn.Delete(zl.ReadLock, state.Version)
		if err != nil {
			panic(err)
		}
		zl.ReadLock = ""
	}
	return nil
}

func (zl *ZKLock) lockWrite() error {
	wg := sync.WaitGroup{}
	var err error
	thisLockNodeBuilder := zl.lockName + "/" + string(WRITE) + "-"
	zl.WriteLock, err = zl.conn.Create(thisLockNodeBuilder, nil, zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil {
		panic(err)
	}
	wg.Add(1)
	// find the write lock before this readlock
	tmp_nodes, _, err := zl.conn.Children(zl.lockName)
	if err != nil {
		panic(err)
	}
	sort.Sort(nodeSlice(tmp_nodes))
	fmt.Println("Write nodes", tmp_nodes)
	for i := len(tmp_nodes) - 1; i >= 0; i-- {
		if zl.WriteLock == zl.lockName+"/"+tmp_nodes[i] {
			if i > 0 {
				// there is still a lock before this write lock
				fmt.Println("write gotcha")
				_, _, event, err := zl.conn.ExistsW(zl.lockName + "/" + tmp_nodes[i-1])
				if err != nil {
					panic(err)
				}
				go func() {
					defer wg.Done()
					e := <-event
					fmt.Println("path: ", e.Path)
				}()
				wg.Wait()
				fmt.Println("get the write lock")
				break
			}
		}
	}
	return nil
}

func (zl *ZKLock) unlockWrite() error {
	if zl.WriteLock != "" {
		// need version of the lock
		_, state, err := zl.conn.Get(zl.WriteLock)
		if err != nil {
			panic(err)
		}
		err = zl.conn.Delete(zl.WriteLock, state.Version)
		if err != nil {
			panic(err)
		}
		zl.WriteLock = ""
	}
	return nil
}

func main() {
	// first create a client and create the root
	zkclient0 := new(ZKLock)
	err := zkclient0.Init([]string{"127.0.0.1:2181"}, "/lockTest")
	if err != nil {
		panic(err)
	}
	wg := sync.WaitGroup{}
	// first one, get the lock and sleep 10s then release the read lock
	wg.Add(1)
	go func() {
		defer wg.Done()
		zkclient := new(ZKLock)
		err := zkclient.Init([]string{"127.0.0.1:2181"}, "/lockTest")
		if err != nil {
			panic(err)
		}
		fmt.Println("test client 1: try to get the write lock")
		zkclient.lockWrite()
		fmt.Println("test client 1: get the write lock")
		time.Sleep(10 * time.Second)
		zkclient.unlockWrite()
		fmt.Println("test client 1: release the write lock after 10 second")
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		zkclient2 := new(ZKLock)
		err := zkclient2.Init([]string{"127.0.0.1:2181"}, "/lockTest")
		if err != nil {
			panic(err)
		}
		fmt.Println("test client 2: try to get the read lock")
		zkclient2.lockRead()
		fmt.Println("test client 2: get the read lock")
		zkclient2.unlockRead()
		fmt.Println("test client 2: release the read lock")
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		// first sleep for a while so other can get the read lock
		zkclient3 := new(ZKLock)
		err := zkclient3.Init([]string{"127.0.0.1:2181"}, "/lockTest")
		if err != nil {
			panic(err)
		}
		time.Sleep(2 * time.Second)
		fmt.Println("test client 3: try to get the write lock")
		zkclient3.lockWrite()
		fmt.Println("test client 3: get the write lock")
		time.Sleep(10 * time.Second)
		zkclient3.unlockWrite()
		fmt.Println("test client 3: release the write lock after 10 second")
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		zkclient4 := new(ZKLock)
		err := zkclient4.Init([]string{"127.0.0.1:2181"}, "/lockTest")
		if err != nil {
			panic(err)
		}
		// delay get read lock
		time.Sleep(5 * time.Second)
		fmt.Println("test client 4: try to get the read lock")
		zkclient4.lockRead()
		fmt.Println("test client 4: get the read lock")
		zkclient4.unlockRead()
		fmt.Println("test client 4: release the read lock")
	}()
	wg.Wait()
	fmt.Println("Test finished")
}

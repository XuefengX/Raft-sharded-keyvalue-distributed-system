package mr

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskIdle = iota
	TaskProcessing
	TaskCommitted
)

type Master struct {
	// Your definitions here.
	count       int
	filenames   []string
	nReduce     int
	mapTasks    []int
	reduceTasks []int
	workers     map[string]int // WorkerID : TaskID
	finished    bool
	timeout     time.Duration
	mu          sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) Work(args *WorkArgs, reply *WorkReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ctx, _ := context.WithTimeout(context.Background(), m.timeout)
	for i, f := range m.filenames {
		if m.mapTasks[i] != TaskIdle {
			continue
		}
		reply.Filename = f
		reply.TaskID = i
		reply.Mode = MapMode
		reply.BucketNum = m.nReduce
		reply.IsFinished = false
		m.mapTasks[i] = TaskProcessing
		m.workers[args.WorkerID] = TaskProcessing
		go func() {
			select {
			case <-ctx.Done():
				{
					m.mu.Lock()
					defer m.mu.Unlock()
					if m.workers[args.WorkerID] != TaskCommitted && m.mapTasks[i] != TaskCommitted {
						m.mapTasks[i] = TaskIdle
						log.Println("ERROR: Worker", args.WorkerID, "with map task:", i, "timeout")
					}
				}
			}
		}()
		return nil
	}

	for i, r := range m.reduceTasks {
		if m.count != len(m.filenames) {
			return nil
		}
		if r != TaskIdle {
			continue
		}
		reply.TaskID = i
		reply.Filename = ""
		reply.BucketNum = len(m.filenames)
		reply.IsFinished = false
		reply.Mode = ReduceMode
		m.workers[args.WorkerID] = TaskProcessing
		m.reduceTasks[i] = TaskProcessing
		go func() {
			select {
			case <-ctx.Done():
				{
					m.mu.Lock()
					defer m.mu.Unlock()
					if m.workers[args.WorkerID] != TaskCommitted && m.reduceTasks[i] != TaskCommitted {
						m.reduceTasks[i] = TaskIdle
						log.Println("ERROR: Worker", args.WorkerID, "with reduce task:", i, "timeout")
					}
				}
			}
		}()
		return nil
	}

	for _, v := range m.workers {
		if v == TaskProcessing {
			reply.IsFinished = false
			return nil
		}
	}

	reply.IsFinished = true
	return errors.New("No more works")
}

func (m *Master) Commit(args *CommitArgs, reply *CommitReply) error {
	m.mu.Lock()
	switch args.Mode {
	case MapMode:
		{
			m.mapTasks[args.TaskID] = TaskCommitted
			m.workers[args.WorkerID] = TaskCommitted
			m.count++
		}
	case ReduceMode:
		m.reduceTasks[args.TaskID] = TaskCommitted
		m.workers[args.WorkerID] = TaskCommitted
	}
	m.mu.Unlock()

	for _, v := range m.mapTasks {
		if v != TaskCommitted {
			return nil
		}
	}

	for _, v := range m.reduceTasks {
		if v != TaskCommitted {
			return nil
		}
	}

	m.finished = true
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	fmt.Print("call example")
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.finished
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		filenames:   files,
		nReduce:     nReduce,
		mapTasks:    make([]int, len(files)),
		reduceTasks: make([]int, nReduce),
		workers:     make(map[string]int),
		finished:    false,
		timeout:     10 * time.Second,
	}

	// Your code here.

	m.server()
	return &m
}

package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu                   sync.Mutex
	nReduce              int
	ret                  bool
	tasks_map            map[string]int
	tasks_map_working    map[string]int
	tasks_reduce         map[string]int
	tasks_reduce_working map[string]int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// function for worker to call a task
func (c *Coordinator) AskTask(args *Request, reply *Reply) error {
	c.mu.Lock()
	//优先分配map任务，再分配reduce任务
	if len(c.tasks_map) > 0 {
		for task, index := range c.tasks_map {
			// fmt.Println("tasks_map:", c.tasks_map)
			reply.TaskType = "map"
			reply.TaskName = task
			reply.Nreduce = c.nReduce
			reply.Index = index
			delete(c.tasks_map, task)
			c.tasks_map_working[task] = index
			go c.timer(reply.TaskType, reply.TaskName)
			c.mu.Unlock()
			return nil
		}
	}
	if len(c.tasks_map_working) == 0 && len(c.tasks_reduce) > 0 {
		// fmt.Println("tasks_reduce:", c.tasks_reduce)
		for task, index := range c.tasks_reduce {
			reply.TaskType = "reduce"
			reply.TaskName = task
			reply.Nreduce = c.nReduce
			reply.Index = index
			//打印reply
			// fmt.Println("reduce_reply:", reply)
			delete(c.tasks_reduce, task)
			c.tasks_reduce_working[task] = index
			go c.timer(reply.TaskType, reply.TaskName)
			c.mu.Unlock()
			return nil
		}
	}
	if len(c.tasks_map)+len(c.tasks_map_working)+len(c.tasks_reduce)+len(c.tasks_reduce_working) == 0 {
		reply.TaskType = "exit"
		c.mu.Unlock()
		return nil
	}
	reply.TaskType = "wait"
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) timer(task_type string, task_name string) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	if task_type == "map" {
		//after 10s task still not complete
		index, exist := c.tasks_map_working[task_name]
		if exist {
			//将"map task failed,reset"信息输出到log文件中
			// log.Printf("map task %v failed,reset\n", task_name)
			delete(c.tasks_map_working, task_name)
			c.tasks_map[task_name] = index
		}
	}
	if task_type == "reduce" {
		index, exist := c.tasks_reduce_working[task_name]
		if exist {
			// log.Printf("reduce task %v failed,reset\n", task_name)
			delete(c.tasks_reduce_working, task_name)
			c.tasks_reduce[task_name] = index
		}
	}
	c.mu.Unlock()
}

func (c *Coordinator) SubmitTask(args *Request, reply *Reply) error{
	c.mu.Lock()
	if args.TaskType == "map" {
		_, exist := c.tasks_map_working[args.TaskName]
		if exist {
			delete(c.tasks_map_working, args.TaskName)
		}
	}
	if args.TaskType == "reduce" {
		_, exist := c.tasks_reduce_working[args.TaskName]
		if exist {
			delete(c.tasks_reduce_working, args.TaskName)
		}
		if len(c.tasks_reduce)+len(c.tasks_reduce_working) == 0 {
			reply.TaskName = "finished"
			c.ret = true
		}
	}
	c.mu.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// print("listening on localhost:1234\n")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		tasks_map:            make(map[string]int),
		tasks_map_working:    make(map[string]int),
		tasks_reduce:         make(map[string]int),
		tasks_reduce_working: make(map[string]int),
	}
	// Your code here.
	for index, file := range files {
		c.tasks_map[file] = index
	}
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("mr-out-%v", i)
		c.tasks_reduce[filename] = i
	}
	c.nReduce = nReduce
	//打印tasks
	// fmt.Println("tasks_map:", c.tasks_map)
	// fmt.Println("tasks_reduce:", c.tasks_reduce)
	c.ret = false
	c.server()
	return &c
}

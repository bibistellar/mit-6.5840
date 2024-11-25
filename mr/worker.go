package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	print("start workers\n")
	// try to connect coordinator
	for {
		args := Request{}
		reply := Reply{}
		ok := call("Coordinator.AskTask", &args, &reply)
		if ok {
			switch reply.TaskType {
			case "map":
				file, _ := os.Open(reply.TaskName)
				content, _ := ioutil.ReadAll(file)
				file.Close()
				key_values := mapf(reply.TaskName, string(content))
				//将key_values根据reduce的数量分为N份
				intermediate := make([][]KeyValue, reply.Nreduce)
				for _, key_value := range key_values {
					index := ihash(key_value.Key) % reply.Nreduce
					intermediate[index] = append(intermediate[index], key_value)
				}
				//将结果写入文件中
				for i := 0; i < reply.Nreduce; i++ {
					file, err := os.CreateTemp("", fmt.Sprintf("map-%d-%d-", reply.Index, i))
					if err != nil {
						fmt.Println("无法创建临时文件:", err)
						return
					}
					defer os.Remove(file.Name()) // 在程序结束时删除临时文件
					enc := json.NewEncoder(file)
					for _, kv := range intermediate[i] {
						enc.Encode(&kv)
					}
					file.Close()
				}
				//向coordinate报告
				args.TaskName = reply.TaskName
				args.TaskType = reply.TaskType
				call("Coordinator.SubmitTask", &args, &reply)
			case "reduce":
				// print("get reduce task,index=",reply.Index,"\n");
				//读取所有map任务的第i个reduce桶
				pattern := fmt.Sprintf("/tmp/map-*-%d-*", reply.Index)
				files, _ := filepath.Glob(pattern)
				intermediate := []KeyValue{}

				//读取中间结果的json，并解析到intermediate中
				for _, file := range files {
					map_file, _ := os.Open(file)
					// print("read file:",map_file.Name(),"\n")
					map_reuslt := json.NewDecoder(map_file)
					for {
						var kv KeyValue
						if err := map_reuslt.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}
					map_file.Close()
				}
				// print("get intermediate\n")
				sort.Sort(ByKey(intermediate))
				ofile, _ := os.Create(reply.TaskName)
				//
				// call Reduce on each distinct key in intermediate[],
				// and print the result to mr-out-0.
				//
				// print("create temp file,intermiediate length = ", len(intermediate),"\n")
				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					// this is the correct format for each line of Reduce output.
					// fmt.Printf("write: %v\n", intermediate[i].Key)
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
					i = j
				}
				ofile.Close()
				//向coordinate报告
				args.TaskName = reply.TaskName
				args.TaskType = reply.TaskType
				ok := call("Coordinator.SubmitTask", &args, &reply)
				if ok && reply.TaskName == "finished" {
					return
				}
			case "exit":
				fmt.Println("No more tasks, exiting.")
				return
			case "wait":
				fmt.Println("No tasks available, waiting.")
				time.Sleep(time.Second)
			default:
				fmt.Println("Unknown task type.")
			}
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	// c, err := rpc.DialHTTP("tcp", "localhost"+":1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

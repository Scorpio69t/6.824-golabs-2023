package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int {
	return len(a)
}
func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func mapper(reply *RequestTaskReply, mapf func(string, string) []KeyValue) {
	filename := reply.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))
	files := []*os.File{}
	encoders := []*json.Encoder{}
	for i := 0; i < reply.N; i++ {
		file, err := ioutil.TempFile("", "")
		if err != nil {
			log.Fatalf("cannot create temp file")
		}
		files = append(files, file)
		encoder := json.NewEncoder(file)
		encoders = append(encoders, encoder)
	}
	for _, kv := range kva {
		i := ihash(kv.Key) % reply.N
		encoders[i].Encode(&kv)
	}
	for i := 0; i < reply.N; i++ {
		oname := fmt.Sprintf("mr-%d-%d", reply.Id, i)
		os.Rename(files[i].Name(), oname)
		files[i].Close()
	}
}

func reducer(reply *RequestTaskReply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for i := 0; i < reply.N; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reply.Id)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	file, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatalf("cannot create temp file")
	}
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
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	oname := fmt.Sprintf("mr-out-%d", reply.Id)
	os.Rename(file.Name(), oname)
	file.Close()
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		requestArgs := RequestTaskArgs{}
		requestReply := RequestTaskReply{}
		finishArgs := FinishTaskArgs{}
		finishReply := FinishTaskReply{}
		if !call("Coordinator.RequestTask", &requestArgs, &requestReply) {
			break
		}
		finishArgs.Id = requestReply.Id
		finishArgs.Type = requestReply.Type
		if requestReply.Type == Map {
			mapper(&requestReply, mapf)
			call("Coordinator.FinishTask", &finishArgs, &finishReply)
		} else if requestReply.Type == Reduce {
			reducer(&requestReply, reducef)
			call("Coordinator.FinishTask", &finishArgs, &finishReply)
		} else if requestReply.Type == Exit {
			break
		} else {
			log.Fatalf("unknown task type %v", requestReply.Type)
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
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

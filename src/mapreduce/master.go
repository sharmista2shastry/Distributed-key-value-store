package mapreduce

import (
	"container/list"
	"fmt"
	"log"
	"sync"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if !ok {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) GetNextWorker(Workers map[string]*WorkerInfo) string {
	var mux sync.Mutex // Create a mutex

	worker := <-mr.registerChannel

	mux.Lock() // Lock the mutex
	_, present := mr.Workers[worker]
	if !present {
		mr.Workers[worker] = &WorkerInfo{}
	}
	mux.Unlock() // Unlock the mutex

	return worker
}

func (mr *MapReduce) DistributeMapJob(args *DoJobArgs, worker string, mapComplete chan bool, Workers map[string]*WorkerInfo) {
	var reply DoJobReply

	ok := call(worker, "Worker.DoJob", args, &reply)

	if ok {
		mapComplete <- true
		mr.registerChannel <- worker
	} else {
		log.Printf("Worker %s failed Map task\n", worker)
		delete(Workers, worker)
		worker := mr.GetNextWorker(Workers)
		mr.DistributeMapJob(args, worker, mapComplete, Workers)
	}
}

func (mr *MapReduce) DistributeReduceJob(args *DoJobArgs, worker string, reduceComplete chan bool, Workers map[string]*WorkerInfo) {
	var reply DoJobReply

	ok := call(worker, "Worker.DoJob", args, &reply)

	if ok {
		reduceComplete <- true
		mr.registerChannel <- worker
	} else {
		log.Printf("Worker %s failed Reduce task\n", worker)
		delete(Workers, worker)
		worker := mr.GetNextWorker(Workers)
		mr.DistributeMapJob(args, worker, reduceComplete, Workers)
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	mr.Workers = make(map[string]*WorkerInfo)

	mapComplete := make(chan bool, mr.nMap)
	reduceComplete := make(chan bool, mr.nReduce)

	for i := 0; i < mr.nMap; i++ {
		go func(jobNum int) {
			worker := mr.GetNextWorker(mr.Workers)

			args := &DoJobArgs{mr.file, Map, jobNum, mr.nReduce}

			mr.DistributeMapJob(args, worker, mapComplete, mr.Workers)
		}(i)
	}

	for i := 0; i < mr.nMap; i++ {
		<-mapComplete
	}

	for i := 0; i < mr.nReduce; i++ {
		go func(jobNum int) {
			worker := mr.GetNextWorker(mr.Workers)

			args := &DoJobArgs{mr.file, Reduce, jobNum, mr.nMap}

			mr.DistributeReduceJob(args, worker, reduceComplete, mr.Workers)
		}(i)
	}

	for i := 0; i < mr.nReduce; i++ {
		<-reduceComplete
	}

	return mr.KillWorkers()
}

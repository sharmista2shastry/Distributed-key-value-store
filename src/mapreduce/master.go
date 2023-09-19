package mapreduce

import (
	"container/list"
	"fmt"
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

func (mr *MapReduce) GetNextWorker() string {

	worker := <-mr.registerChannel

	mr.mux.Lock()
	_, present := mr.Workers[worker]
	if !present {
		mr.Workers[worker] = &WorkerInfo{}
	}
	mr.mux.Unlock()

	return worker
}

func (mr *MapReduce) DistributeMapJob(args *DoJobArgs, worker string, mapComplete chan bool) {
	var reply DoJobReply

	mr.mux.Lock()
	success := call(worker, "Worker.DoJob", args, &reply)
	mr.mux.Unlock()

	if success {
		mapComplete <- true
		mr.registerChannel <- worker
		return
	} else {
		mr.mux.Lock()
		delete(mr.Workers, worker)
		mr.mux.Unlock()
		worker := mr.GetNextWorker()
		mr.DistributeMapJob(args, worker, mapComplete)
	}
}

func (mr *MapReduce) DistributeReduceJob(args *DoJobArgs, worker string, reduceComplete chan bool) {
	var reply DoJobReply

	mr.mux.Lock()
	success := call(worker, "Worker.DoJob", args, &reply)
	mr.mux.Unlock()

	if success {
		reduceComplete <- true
		mr.registerChannel <- worker
		return
	} else {
		mr.mux.Lock()
		delete(mr.Workers, worker)
		mr.mux.Unlock()
		worker := mr.GetNextWorker()
		mr.DistributeReduceJob(args, worker, reduceComplete)
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	mr.Workers = make(map[string]*WorkerInfo)

	mapComplete := make(chan bool, mr.nMap)
	reduceComplete := make(chan bool, mr.nReduce)

	for i := 0; i < mr.nMap; i++ {
		worker := mr.GetNextWorker()

		args := &DoJobArgs{mr.file, Map, i, mr.nReduce}

		go mr.DistributeMapJob(args, worker, mapComplete)
	}

	for i := 0; i < mr.nMap; i++ {
		<-mapComplete
	}

	for i := 0; i < mr.nReduce; i++ {
		worker := mr.GetNextWorker()

		args := &DoJobArgs{mr.file, Reduce, i, mr.nMap}

		go mr.DistributeReduceJob(args, worker, reduceComplete)
	}

	for i := 0; i < mr.nReduce; i++ {
		<-reduceComplete
	}

	return mr.KillWorkers()
}

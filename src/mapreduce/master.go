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

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	mr.Workers = make(map[string]*WorkerInfo)
	var mux sync.Mutex // Create a mutex

	mapComplete := make(chan bool, mr.nMap)
	reduceComplete := make(chan bool, mr.nReduce)

	for i := 0; i < mr.nMap; i++ {
		go func(jobNum int) {
			worker := <-mr.registerChannel

			mux.Lock() // Lock the mutex
			_, present := mr.Workers[worker]
			if !present {
				mr.Workers[worker] = &WorkerInfo{}
			}
			mux.Unlock() // Unlock the mutex

			args := &DoJobArgs{}
			args.File = mr.file
			args.Operation = Map
			args.JobNumber = jobNum
			args.NumOtherPhase = mr.nReduce

			var reply DoJobReply

			ok := call(worker, "Worker.DoJob", args, &reply)

			if ok {
				mapComplete <- true
				mr.registerChannel <- worker
			} else {
				log.Printf("Error: Worker %v failed the given task %v", worker, args.JobNumber)
			}
		}(i)
	}

	for i := 0; i < mr.nMap; i++ {
		<-mapComplete
	}

	log.Printf("Finish Map...\n")

	for i := 0; i < mr.nReduce; i++ {
		go func(jobNum int) {
			worker := <-mr.registerChannel

			mux.Lock() // Lock the mutex
			_, present := mr.Workers[worker]
			if !present {
				mr.Workers[worker] = &WorkerInfo{}
			}
			mux.Unlock() // Unlock the mutex

			args := &DoJobArgs{}
			args.File = mr.file
			args.Operation = Reduce
			args.JobNumber = jobNum
			args.NumOtherPhase = mr.nMap

			var reply DoJobReply

			ok := call(worker, "Worker.DoJob", args, &reply)

			if ok {
				reduceComplete <- true
				mr.registerChannel <- worker
			} else {
				log.Printf("Error: Worker %v failed the given task %v", worker, args.JobNumber)
			}
		}(i)
	}

	for i := 0; i < mr.nReduce; i++ {
		<-reduceComplete
	}

	log.Printf("Finish Reduce...\n")

	return mr.KillWorkers()
}

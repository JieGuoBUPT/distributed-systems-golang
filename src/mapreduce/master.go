package mapreduce

import "container/list"
import "fmt"
import "sync"

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
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	mr.runPhase(Map)
	mr.runPhase(Reduce)
	return mr.KillWorkers()
}

func (mr *MapReduce) runPhase(operation JobType) {
	var numJobs, numOtherPhase int
	switch operation {
		case Map:
			numJobs, numOtherPhase = mr.nMap, mr.nReduce
		case Reduce:
			numJobs, numOtherPhase = mr.nReduce, mr.nMap
	}

	var waitGroup sync.WaitGroup
	waitGroup.Add(numJobs)

	for i := 0; i < numJobs; i++ {
		args := &DoJobArgs {
			File: mr.file,
			Operation: operation,
			JobNumber: i,
			NumOtherPhase: numOtherPhase
		}

		go mr.doJob(args, &waitGroup)
	}

	waitGroup.Wait()
}

func (mr *MapReduce) doJob(args *DoJobArgs, waitGroup *sync.WaitGroup) {
	worker := < -mr.registerChannel
	var reply DoJobReply
	ok := call(worker, "Worker.DoJob", args, & reply)
	if ok == false {
		mr.doJob(args, waitGroup)
	} else {
		waitGroup.Done()
		mr.registerChannel < -worker
	}
}
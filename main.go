package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	// "io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/go-chi/chi"
	"github.com/stasmoon/blockchain_demo/workers"
)

type Block struct {
	Index     int
	Timestamp string
	Msg       string
	Hash      string
	PrevHash  string
}

var Blockchain []Block

type Job struct {
	Msg string
}

type Worker struct {
	workers.Worker
}

type Request struct {
	Msg string
}

func (w *Worker) Process(data []byte) error {
	var job Job
	err := json.Unmarshal(data, &job)
	if err != nil {
		return err
	}

	newBlock, err := generateBlock(Blockchain[len(Blockchain)-1], job.Msg)
	if err != nil {
		log.Println(err)
		return err
	}
	spew.Dump(newBlock)

	if isBlockValid(newBlock, Blockchain[len(Blockchain)-1]) {
		newBlockchain := append(Blockchain, newBlock)
		replaceChain(newBlockchain)
		spew.Dump(Blockchain)
	}

	return nil
}

func createQueue(clientName string) *workers.WorkerQueue {
	return workers.CreateWorkerQueue("blockchaindemo", clientName, "Queue", clientName)
}

func handler(w http.ResponseWriter, r *http.Request) {
	var req Request
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	jobName := "job"
	if h := os.Getenv("JOB_NAME"); h != "" {
		jobName = h
	}
	q := createQueue(jobName)
	q.InitNats()
	defer q.CloseConnectNats()
	q.Send(&Job{
		Msg: req.Msg,
	})

	w.WriteHeader(http.StatusOK)
}

func newRouter() *chi.Mux {
	r := chi.NewRouter()
	r.Post("/", handler)
	return r
}

func main() {
	stopChan := make(chan os.Signal)
	signal.Notify(stopChan, os.Interrupt)

	go func() {
		w := &Worker{}

		workerName := "worker"
		if h := os.Getenv("WORKER_NAME"); h != "" {
			workerName = h
		}

		q := createQueue(workerName)
		q.InitNats()
		q.InitSub(w)
	}()

	t := time.Now()
	startBlock := Block{0, t.String(), "", "", ""}
	spew.Dump(startBlock)
	Blockchain = append(Blockchain, startBlock)

	r := newRouter()

	srv := &http.Server{Addr: ":3000", Handler: r}
	go func() {
		// service connections
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("listen: %s\n", err)
		}
	}()

	<-stopChan
	log.Println("Server gracefully stopped")
}

func isBlockValid(newBlock, oldBlock Block) bool {
	if oldBlock.Index+1 != newBlock.Index {
		return false
	}

	if oldBlock.Hash != newBlock.PrevHash {
		return false
	}

	if calculateHash(newBlock) != newBlock.Hash {
		return false
	}

	return true
}

func replaceChain(newBlocks []Block) {
	if len(newBlocks) > len(Blockchain) {
		Blockchain = newBlocks
	}
}

func calculateHash(block Block) string {
	record := string(block.Index) + block.Timestamp + string(block.Msg) + block.PrevHash
	h := sha256.New()
	h.Write([]byte(record))
	hashed := h.Sum(nil)
	return hex.EncodeToString(hashed)
}

func generateBlock(oldBlock Block, msg string) (Block, error) {

	var newBlock Block

	t := time.Now()

	newBlock.Index = oldBlock.Index + 1
	newBlock.Timestamp = t.String()
	newBlock.Msg = msg
	newBlock.PrevHash = oldBlock.Hash
	newBlock.Hash = calculateHash(newBlock)

	return newBlock, nil
}

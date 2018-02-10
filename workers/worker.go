package workers

import (
	"encoding/json"
	"log"
	"os"

	"github.com/nats-io/go-nats-streaming"
)

type Worker interface {
	Process([]byte) error
	Subscribe(string)
}

type WorkerJob interface{}

type WorkerQueue struct {
	cluster_name string
	client_name  string
	queue_name   string
	durable_name string
	sc           stan.Conn
}

func (wq *WorkerQueue) InitNats() {
	if wq.sc == nil {
		if err := wq.connectNats(); err != nil {
			log.Fatal("Cannot connect nats: " + err.Error())
		}
	}
	return
}

func (wq *WorkerQueue) CloseConnectNats() {
	if wq.sc != nil {
		if err := wq.sc.Close(); err != nil {
			log.Fatal("Cannot close connect nats: " + err.Error())
		}
	}
	return
}

func (wq *WorkerQueue) InitSub(w Worker) error {
	wq.sc.Subscribe(wq.queue_name, func(m *stan.Msg) {
		w.Process(m.Data)
	}, stan.DurableName(wq.durable_name))
	return nil
}

func (wq *WorkerQueue) Send(job WorkerJob) error {
	encoded, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return wq.sc.Publish(wq.queue_name, encoded)
}

func (wq *WorkerQueue) connectNats() (err error) {
	log.Println("Connect nats to " + wq.cluster_name)

	natsURL := "nats://localhost:4222"
	if h := os.Getenv("NATS_URL"); h != "" {
		natsURL = h
	}
	wq.sc, err = stan.Connect(
		wq.cluster_name,
		wq.client_name,
		stan.NatsURL(natsURL),
	)
	if err != nil {
		return err
	}

	log.Println("Connected")

	return err
}

func CreateWorkerQueue(cluster_name, client_name, queue_name, durable_name string) *WorkerQueue {
	return &WorkerQueue{cluster_name: cluster_name, client_name: client_name, queue_name: queue_name, durable_name: durable_name}
}

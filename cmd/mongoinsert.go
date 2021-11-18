package main

import (
	"context"
	"flag"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

var (
	batch = flag.String("batch", "16", "batch size")
	count = flag.String("count", "10000", "count")
)

var mutex sync.Mutex
var total int32
var timeCount int32

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

type OP struct {
	Id        primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	Operation string             `json:"operation"`
}

type Feature struct {
	Id   primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	Data string             `json:"data"`
}

func Log() {
	mutex.Lock()
	log.Printf("cost time :  %f(s) \n total ops : %f(ops) \n iops : %f(ops/s) \n throughput: %f(MB/s)", timeCount*5, total, float32(total)/float32(timeCount*5), float32(total)*4/1024*float32(timeCount*5))
	mutex.Unlock()
}

func main() {
	_, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	flag.Parse()
	batch_number, err := strconv.Atoi(*batch)
	if err != nil {
		log.Fatalf("err %v", err)
		return
	}

	coutNumber, err := strconv.Atoi(*count)

	credential := options.Credential{
		Username: "mercury",
		Password: "B*KTLFJ#d8",
	}

	clientOpts := options.Client().ApplyURI("mongodb://mercurycloud-mongo.cluster-cug6uxa3sg6y.us-west-2.docdb.amazonaws.com:27017").SetAuth(credential)
	client, err := mongo.Connect(context.TODO(), clientOpts)
	if err != nil {
		return
	}

	collection_op := client.Database("raft").Collection("op")
	collection_status := client.Database("raft").Collection("feature")

	wc := writeconcern.New(writeconcern.WMajority())
	rc := readconcern.Snapshot()
	txnOpts := options.Transaction().SetWriteConcern(wc).SetReadConcern(rc)

	var ops []interface{}
	for i := 0; i < batch_number; i++ {
		ops = append(ops, OP{Operation: RandStringRunes(2048)})
	}

	var features []interface{}
	for i := 0; i < batch_number; i++ {
		features = append(features, Feature{Data: RandStringRunes(2048)})
	}
	for i := 0; i < batch_number; i++ {
		ops = append(ops, OP{Operation: RandStringRunes(2048)})
	}

	ticker := time.NewTicker(time.Second * 5)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				timeCount++
				Log()
			}
		}
	}()

	go func() {
		for true {
			session, err := client.StartSession()
			if err != nil {
				panic(err)
			}
			defer session.EndSession(context.Background())
			err = mongo.WithSession(context.Background(), session, func(sessionContext mongo.SessionContext) error {
				if err = session.StartTransaction(txnOpts); err != nil {
					return err
				}
				collection_op.InsertMany(sessionContext, ops)
				if err != nil {
					return err
				}

				collection_status.InsertMany(sessionContext, features)
				if err != nil {
					return err
				}
				if err != nil {
					return err
				}
				if err = session.CommitTransaction(sessionContext); err != nil {
					return err
				}

				return nil
			})
			if err != nil {
				if abortErr := session.AbortTransaction(context.Background()); abortErr != nil {
					panic(abortErr)
				}
				panic(err)
			}

			total = total + 2*int32(batch_number)
			if total >= int32(coutNumber) {
				ticker.Stop()
				done <- true
				Log()
				return
			}
		}
	}()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGUSR1)
	for {
		s := <-ch
		switch s {
		case syscall.SIGQUIT:
			Log()
			return
		}
	}
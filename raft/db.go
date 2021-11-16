package raft

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoClient struct {
	client            *mongo.Client
	collection_op     *mongo.Collection
	collection_status *mongo.Collection
}

func GetMongoClient() (*MongoClient, error) {
	_, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	//credential := options.Credential{
	//	Username: "maodeyi",
	//	Password: "!@#$%^&*()",
	//}

	//clientOpts := options.Client().ApplyURI("mongodb://10.198.23.141:27017")//.SetAuth(credential)
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://10.198.23.141:27017"))
	if err != nil {
		return nil, err
	}

	collection_op := client.Database("raft").Collection("op")
	collection_status := client.Database("raft").Collection("server_status")
	return &MongoClient{
		client:            client,
		collection_op:     collection_op,
		collection_status: collection_status,
	}, err
}

func (m *MongoClient) Close() {
	if err := m.client.Disconnect(context.Background()); err != nil {
		panic(err)
	}
}

type OP struct {
	Id        primitive.ObjectID  `json:"id" bson:"_id,omitempty"`
	Operation string              `json:"operation"`
	TimeStamp primitive.Timestamp `json:"timestamp"`
}

func (m *MongoClient) InsertOpLog() error {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := m.collection_op.InsertOne(ctx, bson.D{{"operation", "add"}, {"timestamp", primitive.Timestamp{T: uint32(time.Now().Unix())}}})
	return err
}

func (m *MongoClient) GetOplog() (*OP, error) {
	result := OP{}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := m.collection_op.FindOne(ctx, bson.M{"timestamp": bson.M{"$gt": primitive.Timestamp{T: uint32(time.Now().Unix())}}}).Decode(&result)
	return &result, err
}

type ServerStatus struct {
	Id       primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	ServerId string             `json:"server_id"`
	SeqId    int32              `json:"seq_id"`
}

func (m *MongoClient) SaveServerStatus(server_id string, seq_id int32) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err := m.collection_status.InsertOne(ctx, bson.D{{"server_id", server_id}, {"seq_id", seq_id}})
	return err
}

func (m *MongoClient) LoadServerStatus(server_id string) (*ServerStatus, error) {
	result := ServerStatus{}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err := m.collection_op.FindOne(ctx, bson.M{"server_id": bson.M{"$eq": server_id}}).Decode(&result)
	return &result, err
}

package db

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	DB_CREATE   = "db_create"
	DB_DEL      = "db_del"
	FEATURE_ADD = "feature_add"
	FEATURE_DEL = "feature_del"
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
	ColId     string              `json:"colid"`
	Operation string              `json:"operation"`
	TimeStamp primitive.Timestamp `json:"timestamp"`
	Feature   string              `json:"feature"`
	Key       string              `json:"key"`
	FeatureId string              `json:"featureid"`
	SeqId     int64               `json:"seqid"`
}

func (m *MongoClient) InsertOpLog(colId, action string, features, featureIds, keys []string, seqIds []int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var doc []interface{}
	var err error
	if action == DB_CREATE || action == DB_DEL {
		op := &OP{
			ColId:     colId,
			Operation: action,
			TimeStamp: primitive.Timestamp{T: uint32(time.Now().Unix())},
			Feature:   "",
			FeatureId: "",
			Key:       "",
			SeqId:     seqIds[0],
		}
		_, err = m.collection_op.InsertOne(ctx, op)
	} else {
		for index, value := range features {
			doc = append(doc, &OP{
				ColId:     colId,
				Operation: action,
				TimeStamp: primitive.Timestamp{T: uint32(time.Now().Unix())},
				Feature:   value,
				FeatureId: featureIds[index],
				Key:       keys[index],
				SeqId:     seqIds[index],
			})
		}
		_, err = m.collection_op.InsertMany(ctx, doc)
	}
	return err
}

func (m *MongoClient) GetOplog(begin int64) (*OP, error) {
	var result OP
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	//cursor, err:= m.collection_op.Find(ctx, bson.M{"seqid": bson.M{"$gt": begin}})
	//if err != nil {
	//	return nil, err
	//}
	//for cursor.Next(ctx) {
	//	op := &OP{}
	//	err := cursor.Decode(op)
	//	if err != nil {
	//		return nil, err
	//	}
	//	results = append(results, op)
	//}
	err := m.collection_op.FindOne(ctx, bson.M{"seqid": bson.M{"$gt": begin}}).Decode(&result)
	return &result, err
}

type ClusterConfig struct {
	Id        primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	WorkerNum int32              `json:"worker_num"`
}

func (m *MongoClient) SaveClusterConfig(workerNum int32) error {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := m.collection_status.InsertOne(ctx, bson.D{{"worker_num", workerNum}})
	return err
}

func (m *MongoClient) LoadClusterConfig() (*ClusterConfig, error) {
	result := ClusterConfig{}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err := m.collection_op.FindOne(ctx, bson.M{}).Decode(&result)
	return &result, err
}

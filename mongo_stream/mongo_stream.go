package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"log"
	"strings"
)

func subscribe(colName string) {

}

func main() {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://10.198.23.141:27017"))
	if err != nil {
		panic(err)
	}
	defer client.Disconnect(context.TODO())
	database := client.Database("raft")
	opCollection := database.Collection("op")

	matchStage := bson.D{{"$match", bson.D{{"operationType", bson.D{{"$in", bson.A{"insert", "update", "replace"}}}}}}}
	//projectStage := bson.D{{"$project", bson.M{"_id": 1, "fullDocument": 1, "ns": 1, "documentKey": 1}}}

	tokenDoc := &bsoncore.Document{}
	opts := options.FindOne()
	opts.SetSort(bson.D{{"$natural", -1}})
	err = client.Database("raft").Collection("op").FindOne(context.TODO(), bson.D{{}}, opts).Decode(&tokenDoc)

	if err != nil {
		log.Println(err)
	}
	//changeStreamDocument
	//object_id := bson.D{{"_id", "618b6476899dc7650a36a54c"}}
	//object_id := bson.Raw{byte[]("618b6476899dc7650a36a54c")}
	resumeToken := strings.Trim(tokenDoc.Lookup("_data").String(), "\"")
	log.Println(resumeToken)
	streamopts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	streamopts.SetResumeAfter(tokenDoc)                                                   //(bson.D{{"_data", resumeToken}})
	episodesStream, err := opCollection.Watch(context.TODO(), mongo.Pipeline{matchStage}) //, streamopts)
	if err != nil {
		log.Println(err)
	}
	defer episodesStream.Close(context.TODO())

	for episodesStream.Next(context.TODO()) {
		var data bson.M
		if err := episodesStream.Decode(&data); err != nil {
			panic(err)
		}

		fmt.Printf("%v\n", data)
	}
}

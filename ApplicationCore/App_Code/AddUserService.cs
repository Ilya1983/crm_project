using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.ServiceModel.Web;
using System.Text;
using MongoDB.Bson;
using MongoDB.Driver;
using Confluent.Kafka;
using System.Activities.Expressions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

// NOTE: You can use the "Rename" command on the "Refactor" menu to change the class name "Service" in code, svc and config file together.
public class AddUserService : IAddUserService
{
    readonly string connectionString = "mongodb://localhost:27017";
    readonly string databaseName = "db";
    readonly string collectionName = "collection";
    readonly string kafkaTopic = "added - users";


    private MongoClient mongoClient;
    private IMongoDatabase db;
    private IMongoCollection<BsonDocument> collection = null;
    private bool mongoReconnect = true;
    private object mongoLocker = new object();

    private ProducerConfig kafkaConfig = new ProducerConfig
    {
        BootstrapServers = "localhost:9092"  // Replace with your Kafka broker address
    };

    private IProducer<Null, string> kafkaProducer;
    private bool kafkaReconnect = true;
    private object kafkaLocker = new object();

    public bool AddUser(string name, int age)
    {
        try
        {
            bool success = AddUserToMongoDB(name, age);
            if (!success)
                return false;

            InsertKafkaMessage(name, age);
            return true;
        }
		catch(Exception ex)
        {
            //write log
            return false;
        }
	}

    private void InsertKafkaMessage(string name, int age)
    {
        IProducer<Null, string> kafkaProducer = GetKafkaProducer();

        JObject messageJson = new JObject();
        messageJson["name"] = name;
        messageJson["age"] = age;

        kafkaProducer.Produce(kafkaTopic, new Message<Null, string>{
            Value = messageJson.ToString()
            });
    }

    private IProducer<Null, string> GetKafkaProducer()
    {
        if(kafkaReconnect || kafkaProducer == null)
        {
            lock (kafkaLocker)
            {
                if (kafkaReconnect == false && kafkaProducer != null)
                    return kafkaProducer;

                kafkaProducer = new ProducerBuilder<Null, string>(kafkaConfig).Build();
                kafkaReconnect = false;
                return kafkaProducer;
            }
        }

        return kafkaProducer;
    }

    private bool AddUserToMongoDB(string name, int age)
    {
        try
        {
            BsonDocument doc = new BsonDocument()
            {
                {"name", name},
                {"age", age}
            };

            IMongoCollection<BsonDocument> collection = GetCollection();
            collection.InsertOneAsync(doc);
            return true;
        }
        catch(Exception ex)
        {
            mongoReconnect = true;
            return false;
        }        
    }

    private IMongoCollection<BsonDocument> GetCollection()
    {
        if(mongoReconnect || collection == null)
        {
            lock(mongoLocker)
            {
                if (mongoReconnect == false && collection != null)
                    return collection;

                mongoClient = new MongoClient(connectionString);
                db = mongoClient.GetDatabase(databaseName);
                collection = db.GetCollection<BsonDocument>(collectionName, null);
                mongoReconnect = false;
                return collection;
            }
        }

        return collection;
    }
}

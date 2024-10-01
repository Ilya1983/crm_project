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

// NOTE: You can use the "Rename" command on the "Refactor" menu to change the class name "Service" in code, svc and config file together.
public class AddUserService : IAddUserService
{
    readonly string connectionString = "mongodb://localhost:27017";
    readonly string databaseName = "db";
    readonly string collectionName = "collection";    

	private MongoClient mongoClient;
    private IMongoDatabase db;
    private IMongoCollection<BsonDocument> collection = null;
    private bool reconnect = true;
    private object locker = new object();

    public bool AddUser(string name, int age)
    {
		bool success = AddUserToMongoDB(name, age);
        if (!success)
            return false;

        InsertKafkaMessage(name, age);
        return true;
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
            reconnect = true;
            return false;
        }        
    }

    private IMongoCollection<BsonDocument> GetCollection()
    {
        if(reconnect || collection == null)
        {
            lock(locker)
            {
                if (reconnect == false && collection != null)
                    return collection;

                mongoClient = new MongoClient(connectionString);
                db = mongoClient.GetDatabase(databaseName);
                collection = db.GetCollection<BsonDocument>(collectionName, null);
                reconnect = false;
                return collection;
            }
        }

        return collection;
    }
}

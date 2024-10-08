using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using System.Collections.Concurrent;
using static Confluent.Kafka.ConfigPropertyNames;
using System.Threading;
using Npgsql;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Replicator
{
    internal class Program
    {
        readonly static string kafkaTopic = "added-users";
        readonly static string connectionString = "Host=localhost;Port=5432;Username=postgres;Password=123456;Database=postgres";

        static private ConsumerConfig kafkaConfig = new ConsumerConfig
        {
            GroupId = "test-consumer-group",
            BootstrapServers = "localhost:9092", // Replace with your Kafka broker
            AutoOffsetReset = AutoOffsetReset.Earliest,  // Start reading at the beginning of the topic
            EnablePartitionEof = false
        };
        static private bool Running = true;
        static private CancellationTokenSource cancelSource = new CancellationTokenSource();

        static void Main(string[] args)
        {
            Running = true;
            Task task = Task.Run(ListenToKafkaQueue);
            Console.ReadLine();
            Running = false;
            cancelSource.Cancel();
            task.Wait();
        }

        static void ListenToKafkaQueue()
        {
            using(IConsumer<Ignore, string> consumer = new ConsumerBuilder<Ignore, string>(kafkaConfig).Build())
            {
                consumer.Subscribe(kafkaTopic);

                while(Running)
                {
                    try
                    {
                        ConsumeResult<Ignore, string> consumerResult = consumer.Consume(cancelSource.Token);
                        if (Running)
                            InsertNewUser(consumerResult.Message.Value);
                    }
                    catch(Exception ex)
                    {
                        Console.WriteLine($"consume failed due to the following exception {ex}");
                    }
                }
            }
        }

        private static void InsertNewUser(string message)
        {
            JObject messageObject = JObject.Parse(message);
            string name = (string)messageObject["name"];
            int age = (int)messageObject["age"];

            using(NpgsqlConnection conn = new NpgsqlConnection(connectionString))
            {
                conn.Open();
                string insertQuery = "INSERT INTO users (name, age) VALUES (@name, @age)";
                using(NpgsqlCommand cmd = new NpgsqlCommand(insertQuery, conn))
                {
                    cmd.Parameters.AddWithValue("name", name);
                    cmd.Parameters.AddWithValue("age", age);

                    int affectedRows = cmd.ExecuteNonQuery();
                    if (affectedRows > 0)
                        Console.WriteLine("command succeeded");
                    else
                        Console.WriteLine("command failed");
                }
            }
        }
    }
}

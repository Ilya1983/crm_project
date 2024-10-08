using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Replicator
{
    internal class KafkaListener
    {
        readonly string kafkaTopic = "added - users";

        private ProducerConfig kafkaConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"  // Replace with your Kafka broker address
        };

        private event MessageArrivedHandler messageArrived;

        private IConsumer<Null, string> kafkaConsumer;
        private bool Running = false;
        private CancellationTokenSource cancelToken;

        public delegate void MessageArrivedHandler(string message, EventArgs e);

        public KafkaListener()
        {
            cancelToken = new CancellationTokenSource();
        }

        public void subsribeOnMessage(MessageArrivedHandler handler)
        {
            messageArrived += handler;
        }

        public void Init()
        {
            kafkaConsumer = new ConsumerBuilder<Null, string>(kafkaConfig).Build();
            kafkaConsumer.Subscribe(kafkaTopic);

            Running = true;
            Task.Run(() =>
            {
                while(Running)
                {
                    ConsumeResult<Null, string> message = kafkaConsumer.Consume(cancelToken.Token);
                    messageArrived(message.Message.Value, new EventArgs());
                }
            });
        }

        public void Stop()
        {
            cancelToken.Cancel();
            Running = false;
        }

    }
}

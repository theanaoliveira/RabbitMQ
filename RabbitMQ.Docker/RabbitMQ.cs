using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ.Docker
{
    public class RabbitMQ
    {
        /// <summary>
        /// Objeto de conexão com a fila
        /// </summary>
        private static IConnection ConnectionMQ;

        /// <summary>
        /// Construtor
        /// </summary>
        public RabbitMQ()
        {
            if (ConnectionMQ == null)
                ConnectionMQ = GetConnection();
        }

        /// <summary>
        /// Encera a conexão da fila
        /// </summary>
        public void CloseConnection()
        {
            if (ConnectionMQ != null)
            {
                ConnectionMQ.Close();
                ConnectionMQ.Dispose();
            }
        }

        /// <summary>
        /// Inicia a conexão com a fila
        /// </summary>
        /// <returns></returns>
        private static IConnection GetConnection()
        {
            var lstrAddress = Environment.GetEnvironmentVariable("RABBITMQ_DEFAULT_URL");
            var lstrUser = Environment.GetEnvironmentVariable("RABBITMQ_DEFAULT_USER");
            var lstrPassword = Environment.GetEnvironmentVariable("RABBITMQ_DEFAULT_PASS");

            var loFactory = new ConnectionFactory()
            {
                HostName = lstrAddress,
                UserName = lstrUser,
                Password = lstrPassword,
                Port = 15672
            };

            return loFactory.CreateConnection();
        }

        /// <summary>
        /// Insere um novo item na fila
        /// </summary>
        public void InsertQueue(string item)
        {
            var lstrQueueName = Environment.GetEnvironmentVariable("RABBITMQ_DEFAULT_QUEUE");

            using (var loChannel = ConnectionMQ.CreateModel())
            {
                var loBody = Encoding.UTF8.GetBytes(item);

                loChannel.QueueDeclare(queue: lstrQueueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
                loChannel.BasicPublish(exchange: "", routingKey: lstrQueueName, basicProperties: null, body: loBody);
            }
        }

        /// <summary>
        /// Le a fila
        /// </summary>
        public void ReadQueue()
        {
            //cria a canal de comunicação com o rabbitmq

            var lstrQueueName = Environment.GetEnvironmentVariable("RABBITMQ_DEFAULT_QUEUE");
            var loChannel = ConnectionMQ.CreateModel();

            Task.Factory.StartNew(() =>
            {
                lock (loChannel)
                {
                    var consumer = new EventingBasicConsumer(loChannel)
                    {
                        ConsumerTag = Guid.NewGuid().ToString() // Tag de identificação do consumidor no RabbitMQ
                    };

                    consumer.Received += (sender, ea) =>
                    {
                        var body = ea.Body;
                        var brokerMessage = Encoding.Default.GetString(ea.Body);

                        //Diz ao RabbitMQ que a mensagem foi lida com sucesso pelo consumidor
                        loChannel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: true);

                        Console.WriteLine(brokerMessage);
                    };

                    //Registra os consumidor no RabbitMQ
                    loChannel.BasicConsume(lstrQueueName, autoAck: false, consumer: consumer);
                }
            });
        }
    }
}

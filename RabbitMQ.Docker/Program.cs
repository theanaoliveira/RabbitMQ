using System;

namespace RabbitMQ.Docker
{
    class Program
    {
        static void Main(string[] args)
        {
            var rabbitMQ = new RabbitMQ();

            for (var i = 0; i < 1000; i++)
                rabbitMQ.InsertQueue($"item_{i}");

            rabbitMQ.ReadQueue();
        }
    }
}

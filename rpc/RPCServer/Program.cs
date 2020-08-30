using System;
using RabbitMQ.Client;
using System.Text;
using RabbitMQ.Client.Events;
namespace RPCServer
{
    class Program
    {
        private static int fib(int n)
        {
            if (n < 2) return n;
            int fb1 = 1;
            int fb0 = 1;
            for (int i = 2; i <= n; ++i)
            {
                int temp = fb1;
                fb1 += fb0;
                fb0 = temp;
            }
            return fb1;
        }
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory();
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "rpc", durable: false, exclusive: false, autoDelete: false, arguments: null);

                channel.BasicQos(0, 1, false);

                var consumer = new EventingBasicConsumer(channel);
                channel.BasicConsume(queue: "rpc", autoAck: false, consumer: consumer);

                Console.WriteLine("Awaiting RPC requests...");

                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var props = ea.BasicProperties;
                    var replyProps = channel.CreateBasicProperties();
                    replyProps.CorrelationId = props.CorrelationId;

                    var message = Encoding.UTF8.GetString(body);
                    int n = int.Parse(message);
                    Console.WriteLine("fib({0})", message);
                    var resp = fib(n).ToString();

                    var responseBytes = Encoding.UTF8.GetBytes(resp);
                    channel.BasicPublish(exchange: "", routingKey: props.ReplyTo, basicProperties: replyProps, body: responseBytes);
                    Console.WriteLine("Replied");
                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                };
                Console.WriteLine("Press [enter] to exit...");
                Console.ReadLine();
            }
        }
    }
}

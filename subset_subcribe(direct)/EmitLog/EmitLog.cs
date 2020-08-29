using System;
using RabbitMQ.Client;
using System.Text;
using System.Linq;
class EmitLog
{
    public static void Main(string[] args)
    {
        var factory = new ConnectionFactory() { HostName = "localhost" };
        using (var connection = factory.CreateConnection())
        using (var channel = connection.CreateModel())
        {
            channel.ExchangeDeclare(exchange: "topic_logs", type: ExchangeType.Topic);
            var queueName = channel.QueueDeclare().QueueName;


            var message = (args.Length > 1 ? string.Join(" ", args.Skip(1).ToArray()) : "Hello world!");
            var body = Encoding.UTF8.GetBytes(message);

            var properties = channel.CreateBasicProperties();
            properties.Persistent = true;
            var severity = args.Length > 0 ? args[0] : "anonymous.info";
            channel.BasicPublish(exchange: "topic_logs",
                                 routingKey: severity,
                                 basicProperties: properties,
                                 body: body);
            Console.WriteLine(" [x] Sent {0}:{1}", severity, message);
        }

        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();
    }

    private static string GetMessage(string[] args)
    {
        return ((args.Length > 0) ? string.Join(" ", args) : "Hello World!");
    }
}
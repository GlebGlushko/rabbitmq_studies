using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
namespace RPCClient
{
    class RPCClient
    {
        private const string QUEUE_NAME = "rpc";
        private readonly IConnection connection;
        private readonly IModel channel;
        private readonly string replyQueueName;
        private readonly EventingBasicConsumer consumer;

        private readonly ConcurrentDictionary<string, TaskCompletionSource<string>> callbackMapper = new ConcurrentDictionary<string, TaskCompletionSource<string>>();

        public RPCClient()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };

            connection = factory.CreateConnection();
            channel = connection.CreateModel();

            replyQueueName = channel.QueueDeclare().QueueName;
            consumer = new EventingBasicConsumer(channel);

            consumer.Received += (model, ea) =>
            {
                Console.WriteLine("Kek");
                if (!callbackMapper.TryRemove(ea.BasicProperties.CorrelationId, out TaskCompletionSource<string> tcs)) return;

                var body = ea.Body.ToArray();
                var resp = Encoding.UTF8.GetString(body);
                tcs.TrySetResult(resp);
            };

        }

        public Task<string> CallAsync(string message, CancellationToken token = default(CancellationToken))
        {
            IBasicProperties props = channel.CreateBasicProperties();
            var correlationId = Guid.NewGuid().ToString();
            props.CorrelationId = correlationId;
            props.ReplyTo = replyQueueName;

            var messageBytes = Encoding.UTF8.GetBytes(message);

            var tcs = new TaskCompletionSource<string>();
            callbackMapper.TryAdd(correlationId, tcs);
            channel.BasicPublish(exchange: "", routingKey: QUEUE_NAME, basicProperties: props, body: messageBytes);
            channel.BasicConsume(consumer: consumer, queue: replyQueueName, autoAck: true);

            token.Register(() => callbackMapper.TryRemove(correlationId, out var temp));
            return tcs.Task;
        }
        public void Close()
        {
            connection.Close();
        }

    }

    public class Rpc
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("RPC client");
            string n = args.Length > 0 ? args[0] : "10";

            Task t = InvokeAsync(n);
            t.Wait();
            Console.WriteLine("Press [enter] to exit...");
            Console.ReadLine();
        }

        private static async Task InvokeAsync(string n)
        {
            var rpcClient = new RPCClient();

            Console.WriteLine("Requesting fib({0})", n);

            var resp = await rpcClient.CallAsync(n);
            Console.WriteLine("Got response {0}", resp);

            rpcClient.Close();
        }
    }
}

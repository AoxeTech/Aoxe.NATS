namespace Aoxe.NATS.Client;

public class Test
{
    public async ValueTask Publish()
    {
        // NATS core M:N messaging example
        await using var nc = new NatsClient();

        // Subscribe on one terminal
        await foreach (var msg in nc.SubscribeAsync<string>(subject: "foo"))
        {
            Console.WriteLine($"Received: {msg.Data}");
        }

        // Start publishing to the same subject on a second terminal
        await nc.PublishAsync(subject: "foo", data: "Hello, World!");
    }

    public async ValueTask JetStream()
    {
        await using var nc = new NatsClient();
        var js = nc.CreateJetStreamContext();

        // Create a stream to store the messages
        await js.CreateStreamAsync(
            new StreamConfig(name: "ORDERS", subjects: new[] { "orders.*" })
        );

        // Publish a message to the stream. The message will be stored in the stream
        // because the published subject matches one of the the stream's subjects.
        var ack = await js.PublishAsync(subject: "orders.new", data: "order 1");
        ack.EnsureSuccess();

        // Create a consumer on a stream to receive the messages
        var consumer = await js.CreateOrUpdateConsumerAsync(
            "ORDERS",
            new ConsumerConfig("order_processor")
        );

        await foreach (var jsMsg in consumer.ConsumeAsync<string>())
        {
            Console.WriteLine($"Processed: {jsMsg.Data}");
            await jsMsg.AckAsync();
        }
    }
}

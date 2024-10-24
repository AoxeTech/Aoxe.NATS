namespace Aoxe.NATS.Client;

public class Test(
    INatsClient natsClient,
    INatsJSContext natsJsContext,
    INatsKVContext natsKvContext,
    INatsObjContext natsObjContext,
    INatsSvcContext natsSvcContext
)
{
    public async ValueTask PublishSubscribeAsync()
    {
        var subscription = Task.Run(async () =>
        {
            await foreach (var msg in natsClient.SubscribeAsync<int>("foo"))
            {
                Console.WriteLine($"Received {msg.Subject}: {msg.Data}\n");
                if (msg.Data == -1)
                    break;
            }
        });

        // Give subscription time to start
        await Task.Delay(1000);

        for (var i = 0; i < 10; i++)
        {
            Console.WriteLine($" Publishing {i}...");
            await natsClient.PublishAsync("foo", i);
        }

        // Signal subscription to stop
        await natsClient.PublishAsync("foo", -1);

        // Make sure subscription completes cleanly
        await subscription;
    }

    public async ValueTask JetStreamAsync()
    {
        // Create a stream to store the messages
        await natsJsContext.CreateStreamAsync(
            new StreamConfig(name: "ORDERS", subjects: ["orders.*"])
        );

        // Publish a message to the stream. The message will be stored in the stream
        // because the published subject matches one of the the stream's subjects.
        var ack = await natsJsContext.PublishAsync(subject: "orders.new", data: "order 1");
        ack.EnsureSuccess();

        // Create a consumer on a stream to receive the messages
        var consumer = await natsJsContext.CreateOrUpdateConsumerAsync(
            "ORDERS",
            new ConsumerConfig("order_processor")
        );

        await foreach (var jsMsg in consumer.ConsumeAsync<string>())
        {
            Console.WriteLine($"Processed: {jsMsg.Data}");
            await jsMsg.AckAsync();
        }
    }

    public async ValueTask KeyValueStoreAsync()
    {
        var store = await natsKvContext.CreateStoreAsync("SHOP_ORDERS");
        await store.PutAsync("order-1", new ShopOrder(Id: 1));

        var entry = await store.GetEntryAsync<ShopOrder>("order-1");

        Console.WriteLine($"[GET] {entry.Value}");

        await foreach (var shopOrder in store.WatchAsync<ShopOrder>())
        {
            Console.WriteLine($"[RCV] {shopOrder}");
        }
    }

    public async ValueTask ObjectStoreAsync()
    {
        var store = await natsObjContext.CreateObjectStoreAsync("test-bucket");
        await store.PutAsync("my/random/data.bin", File.OpenRead("data.bin"));
        await store.GetAsync("my/random/data.bin", File.OpenWrite("data_copy.bin"));

        var metadata = await store.GetInfoAsync("my/random/data.bin");

        Console.WriteLine("Metadata:");
        Console.WriteLine($"  Bucket: {metadata.Bucket}");
        Console.WriteLine($"  Name: {metadata.Name}");
        Console.WriteLine($"  Size: {metadata.Size}");
        Console.WriteLine($"  Time: {metadata.MTime}");
        Console.WriteLine($"  Chunks: {metadata.Chunks}");

        await store.DeleteAsync("my/random/data.bin");
    }

    public async ValueTask ServiceAsync()
    {
        await using var svcServer = await natsSvcContext.AddServiceAsync("test", "1.0.0");
        await svcServer.AddEndpointAsync<int>(
            name: "divide42",
            handler: async m =>
            {
                // Handle exceptions which may occur during message processing,
                // usually due to serialization errors
                if (m.Exception != null)
                {
                    await m.ReplyErrorAsync(500, m.Exception.Message);
                    return;
                }

                if (m.Data == 0)
                {
                    await m.ReplyErrorAsync(400, "Division by zero");
                    return;
                }

                await m.ReplyAsync(42 / m.Data);
            }
        );

        var grp1 = await svcServer.AddGroupAsync("grp1");
        await grp1.AddEndpointAsync<int>(
            name: "ep1",
            handler: async m =>
            {
                // handle message
            }
        );
    }
}

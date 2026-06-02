using LiveObjectTracker.DomainModel.Models;
using System.Net.Sockets;
using System.Text.Json;

Console.WriteLine("=== Live Object Tracker Generator ===");

const string Host = "host.docker.internal"; // TODO через env (127.0.0.1)
const int Port = 5000;
const int EventsPerBatch = 100;
const int DelayBetweenBatchMs = 10; // 100 событий / 10мс = 10_000 Rpc

Console.WriteLine("Press key to start");
Console.ReadLine();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (sender, e) =>
{
    e.Cancel = true;
    Console.WriteLine("\n[Generator] Ctrl + C received. Stopping...");
    cts.Cancel();
};

Console.WriteLine($"Connecting to {Host}:{Port}...");

long sentCount = 0;
try
{
    using var client = new TcpClient();
    await client.ConnectAsync(Host, Port, cts.Token);
    using var stream = client.GetStream();

    var rng = new Random(42);
    ulong id = 0;
    Console.WriteLine($"[Generaot] Connected. Sending {EventsPerBatch * (1000 / DelayBetweenBatchMs)}");

    while (!cts.IsCancellationRequested)
    {
        for (int i = 0; i < EventsPerBatch; i++)
        {
            var evt = ToCoordinateEvent(rng, id);
            var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(evt);
            var lenBytes = BitConverter.GetBytes(jsonBytes.Length);

            await stream.WriteAsync(lenBytes, cts.Token);
            await stream.WriteAsync(jsonBytes, cts.Token);
            sentCount++;
            id++;
        }

        Console.WriteLine($"[Generator] Sent: {sentCount,10} events");

        if (!cts.IsCancellationRequested)
            await Task.Delay(DelayBetweenBatchMs, cts.Token);
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("[Generator] Canelled by token");
}
catch (Exception ex)
{
    Console.WriteLine($"[Generator] Unexcepted Error: {ex.Message}");
}

Console.WriteLine($"[Generator] Total send: {sentCount}. Bye.");

static CoordinateEvent ToCoordinateEvent(Random rng, ulong id)
    => new CoordinateEvent(
                      ObjectId: id,
                      X: (float)rng.NextDouble() * 1000,
                      Y: (float)rng.NextDouble() * 1000,
                      Z: (float)rng.NextDouble() * 1000,
                      Timestamp: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()); ;

Console.ReadLine();
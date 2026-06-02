using LiveObjectTracker.Service.Models;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;

Console.WriteLine("=== Live Object Tracker ===");

// TODO 1.2: TCP-сервер + BoundedChannel (10_000, Wait mode)
// TODO 1.2.1: Добавляем BoundedChannel и базовую структуру

const int TcpPort = 5000;
const int ChannelCapacity = 10_000;
const int WorkersCount = 4; // TODO перейти на Enviroment

//FullMode.Wait->когда канал полон, производитель блокируется (backpressure)
var channel = Channel.CreateBounded<CoordinateEvent>(
    new BoundedChannelOptions(ChannelCapacity)
    {
        FullMode = BoundedChannelFullMode.Wait,
        SingleReader = false,
        SingleWriter = false,
    });


Console.WriteLine($"Config: port={TcpPort}, capacity={ChannelCapacity}, workers={WorkersCount}");

// TCP - сервер
_ = Task.Run(async () =>
{
    var listener = new TcpListener(IPAddress.Loopback, TcpPort);
    listener.Start();
    Console.WriteLine($"[TCP] Listening on port {TcpPort}...");

    while (true)
    {
        var client = await listener.AcceptTcpClientAsync();
        Console.WriteLine($"[TCP] Client connected from {client.Client.RemoteEndPoint}");

        _ = Task.Run(() => HandleClientAsync(client, channel.Writer));
    }
});

Console.WriteLine("[TCP] Server is running");

static async Task HandleClientAsync(TcpClient client, ChannelWriter<CoordinateEvent> writer)
{
    using var stream = client.GetStream(); //Dispose при socket.Closed()
    var buffer = new byte[1024];

    try
    {
        while (true)
        {
            var bytesRead = await stream.ReadAsync(buffer);
            if (bytesRead == 0) break; // клиент отключился

            //TODO: Распарсить JSON и отправить в channel
            Console.WriteLine($"[TCP] Received {bytesRead} bytes");
        }
    }
    finally
    {
        client.Close();
        Console.WriteLine($"[TCP] Client disconnected");
    }
}

// TODO 1.3: Worker pool (TaskCreationOptions.LongRunning)
// TODO 1.4: Generator-client (~10k RPS)
// TODO 1.5: Console metrics (produced/consumed RPS, pending, cache size)
// TODO 2.1-2.2: PositionsCache (ReaderWriterLockSlim)
// TODO 3.3-3.4: Batch writers (Postgres COPY + ClickHouse)

Console.ReadLine();
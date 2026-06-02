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

// Gracefull shutdown
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (sender, e) =>
{
    e.Cancel = true; // Отменяем стандартное значение
    //Интерапт
    Console.WriteLine("\n[Shutdown] Ctrl+C получен. Завершаем...");
    cts.Cancel();
};

//FullMode.Wait->когда канал полон, производитель блокируется (backpressure)
var channel = Channel.CreateBounded<CoordinateEvent>(
    new BoundedChannelOptions(ChannelCapacity)
    {
        FullMode = BoundedChannelFullMode.Wait,
        SingleReader = false,
        SingleWriter = false,
    });


Console.WriteLine($"Config: port={TcpPort}, capacity={ChannelCapacity}, workers={WorkersCount}");

var waitGroup = new AsyncWaitGroup();

// TCP - сервер
var listener = new TcpListener(IPAddress.Loopback, TcpPort);
listener.Start();
Console.WriteLine($"[TCP] Listening on port {TcpPort}...");

try
{
    while (!cts.Token.IsCancellationRequested)
    {
        var client = await listener.AcceptTcpClientAsync();
        Console.WriteLine($"[TCP] Client connected from {client.Client.RemoteEndPoint}");

        waitGroup.Add();

        _ = HandleClientAsync(client, channel.Writer, cts.Token);
    }
}
catch (ObjectDisposedException)
{
    Console.WriteLine($"[TCP] Listner stopped {nameof(ObjectDisposedException)}");
}
catch (SocketException ex) when (ex.SocketErrorCode == SocketError.Interrupted)
{
    Console.WriteLine($"[TCP] Listner stopped {nameof(SocketException)}.{SocketError.Interrupted}");
}
finally
{
    listener.Stop(); //Гарантированно закрываем
    Console.WriteLine("[TCP] Listner disposed.");
}

// Аналог wg.Wait() в Go
Console.WriteLine("[Shutdown] Waiting for active clients to finish (WaitGroup)...");
await waitGroup.WaitAsync();
Console.WriteLine("[Shutdown] All clients disconnected. Server stopped gracefully.");

async Task HandleClientAsync(
    TcpClient client,
    ChannelWriter<CoordinateEvent> writer,
    CancellationToken ct = default)
{
    using var stream = client.GetStream(); //Dispose при socket.Closed()
    var buffer = new byte[1024];

    try
    {
        while (!cts.Token.IsCancellationRequested)
        {
            var bytesRead = await stream.ReadAsync(buffer, ct);
            if (bytesRead == 0) break; // клиент отключился

            //TODO: Распарсить JSON и отправить в channel
            Console.WriteLine($"[TCP] Received {bytesRead} bytes");
        }
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine("[TCP] Client handler cancelled.");
    }
    catch (IOException)
    {
        Console.WriteLine("[TCP] Client connection broken.");
    }
    finally
    {
        waitGroup.Done();

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

//--------------------------------
public sealed class AsyncWaitGroup
{
    private int _count;

    // RunContinuationsAsynchronously нужно, чтобы коллбеки не выполнялись в потоке, вызвавшем Done()
    private readonly TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Add(int count = 1) => Interlocked.Add(ref _count, count);

    public void Done()
    {
        if (Interlocked.Decrement(ref _count) == 0)
        {
            _tcs.TrySetResult();
        }
    }

    public Task WaitAsync() => _tcs.Task;
}
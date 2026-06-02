using LiveObjectTracker.Service.Models;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using System.Linq;

Console.WriteLine("=== Live Object Tracker ===");

// TODO 1.2: TCP-сервер + BoundedChannel (10_000, Wait mode)
// TODO 1.2.1: Добавляем BoundedChannel и базовую структуру
// TODO 1.3: Worker pool (TaskCreationOptions.LongRunning)

const int TcpPort = 5000;
const int ChannelCapacity = 10_000;
const int WorkersCount = 4; // TODO перейти на Enviroment

var listener = new TcpListener(IPAddress.Loopback, TcpPort);

// Gracefull shutdown
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (sender, e) =>
{
    if (cts.IsCancellationRequested)
    {
        Console.WriteLine("\n[Fatal] Повторный Ctrl+C. Жесткое завершение процесса...");
        e.Cancel = false;
        return;
    }

    e.Cancel = true; // Отменяем стандартное значение
    //Интерапт
    Console.WriteLine("\n[Shutdown] Ctrl+C получен. Завершаем...");
    cts.Cancel();
    listener.Stop(); // прерывает ожидание AcceptTcpClientAsync
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
long processedCount = 0; // Счетчик обработанных событий

// Worker pool
Console.WriteLine($"[Workers] Starting worker pool...");

var workers = Enumerable.Range(0, WorkersCount).Select(i =>
{
    return Task.Factory.StartNew(
        async () =>
        {
            Console.WriteLine($"[Worker {i} Started on Thread {Environment.CurrentManagedThreadId}]");

            // Бесконечное асинхронное чтение
            // Цикл прервется сам, когда вызовем channel.Writer.Complete()
            await foreach (var evt in channel.Reader.ReadAllAsync())
            {
                if (cts.IsCancellationRequested) break; // Явно не даем стартануть полезную работу если был interupt

                await Task.Delay(10, CancellationToken.None);

                var count = Interlocked.Increment(ref processedCount);
                Console.WriteLine($"[Worker {i}] Processed Object: {evt.ObjectId} | Total: {count}");
            }

            Console.WriteLine($"[Worker {i}] Channel drained. Stopping");
        },
        CancellationToken.None, // Воркеры не оменяем через токен, они должны ДОЧИТАТЬ канал до конца
        TaskCreationOptions.LongRunning, // Выделенные потоки не обьедаем thread pool
        TaskScheduler.Default).Unwrap();// Разварачиваем Task<Task> иначе пролетим
}).ToArray();

// TCP - сервер
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
catch (Exception ex) when (cts.Token.IsCancellationRequested)
{
    // В процессе выключения. listener.Stop() прервал AcceptTcpClientAsync.
    // Плевать на тип исключения (SocketException, IOException и т.д.), мы просто выходим.
    Console.WriteLine($"[TCP] Listener interrupted during shutdown: {ex.Message}");
}
catch (SocketException ex)
{
    // Если мы НЕ выключались, а сокет упал — это реальная ошибка, логируем её
    Console.WriteLine($"[TCP] Unexpected Socket Error: {ex.Message}");
}
finally
{
    Console.WriteLine("[TCP] Listner disposed.");
}

// Аналог wg.Wait() в Go
Console.WriteLine("[Shutdown] 1. Waiting for active clients...");
await waitGroup.WaitAsync();

Console.WriteLine("[Shutdown] 2. Conpleting channel...");
channel.Writer.Complete(); // говорим воркерам что работы больше нет, ДОЕДАЙТЕ ЧТО ОСТАЛОСЬ и выходите

Console.WriteLine("[Shutdown] 3. Waiting for workers to drain...");
await Task.WhenAll(workers); // Ждем пока все воркеры выйдут из ReadAllASync

Console.WriteLine($"[Shutdown] Done! Total processed: {processedCount}. Bye.");

//--------------------------------
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
            
            if (ct.IsCancellationRequested) break; // Неначинать полезную раблоту если был interupt

            // Для теста: при получении любых байт от клиента, кидаем фейковое событие в канал
            Console.WriteLine($"[TCP] Received {bytesRead} bytes from client.");
            var dummyEvent = new CoordinateEvent(
                ObjectId: (ulong)bytesRead,
                X: 1, Y: 2, Z: 3,
                Timestamp: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

            await writer.WriteAsync(dummyEvent, ct);
        }
    }
    catch (OperationCanceledException) { Console.WriteLine("[TCP] Client handler cancelled."); }
    catch (IOException) { Console.WriteLine("[TCP] Client connection broken."); }
    finally
    {
        waitGroup.Done();

        client.Close();
        Console.WriteLine($"[TCP] Client disconnected");
    }
}

// TODO 1.4: Generator-client (~10k RPS)
// TODO 1.5: Console metrics (produced/consumed RPS, pending, cache size)
// TODO 2.1-2.2: PositionsCache (ReaderWriterLockSlim)
// TODO 3.3-3.4: Batch writers (Postgres COPY + ClickHouse)

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

    public Task WaitAsync()
    {
        if (Volatile.Read(ref _count) == 0)
        {
            return Task.CompletedTask;
        }

        return _tcs.Task;
    }
}
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using System.Linq;
using System.Buffers;
using LiveObjectTracker.DomainModel.Models;
using System.Collections.Concurrent;
using LiveObjectTracker.Service;

Console.WriteLine("=== Live Object Tracker ===");

// TODO 1.2: TCP-сервер + BoundedChannel (10_000, Wait mode)
// TODO 1.2.1: Добавляем BoundedChannel и базовую структуру
// TODO 1.3: Worker pool (TaskCreationOptions.LongRunning)
// TODO 1.4: Generator-client (~10k RPS)
// TODO 1.5: Console metrics (produced/consumed RPS, pending, cache size)
// TODO 2.1-2.2: PositionsCache (ReaderWriterLockSlim)

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
long producedCount = 0;  // Счетчик принятых от клиентов событий
var positionsCache = new PositionsCache(cts.Token);

// Metrics
_ = Task.Run(async () =>
{
    long prevProduced = 0;
    long prevConsumed = 0;

    while (!cts.Token.IsCancellationRequested)
    {
        await Task.Delay(1000, cts.Token);

        var curProduced = Interlocked.Read(ref producedCount);
        var curConsumed = Interlocked.Read(ref processedCount);

        var rpsProduced = curProduced - prevProduced;
        var rpsConsumed = curConsumed - prevConsumed;

        prevProduced = curProduced;
        prevConsumed = curConsumed;

        Console.WriteLine(
            $"[Metrics] Produced: {rpsProduced,6} RPS | " +
            $"Consumed: {rpsConsumed,6} RPS | " +
            $"Pending: {channel.Reader.Count,6} | " +
            $"Cache: {positionsCache.Count,7}");
    }
}, cts.Token);


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
                
                positionsCache.Set(evt);
                var count = Interlocked.Increment(ref processedCount);
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
    const int LengthPrefixSize = 4;
    const int MaxPayloadSize = 64 * 1024; // 64 кб

    using var stream = client.GetStream(); //Dispose при socket.Closed()
    var lenBuf = new byte[LengthPrefixSize];
    var payloadBuffer = ArrayPool<byte>.Shared.Rent(MaxPayloadSize);

    try
    {
        while (!cts.Token.IsCancellationRequested)
        {
            // Читаем префикс длинны (4 байта)
            int totalRead = 0;
            int read;
            while ((read = await stream.ReadAsync(lenBuf.AsMemory(totalRead, 4 - totalRead), ct)) > 0)
            {
                totalRead += read;
                if (totalRead == LengthPrefixSize) break;
            }

            if (read == 0 || ct.IsCancellationRequested) break;

            int payloadLength = BitConverter.ToInt32(lenBuf);
            if (payloadLength <= 0 || payloadLength > MaxPayloadSize)
            {
                Console.WriteLine($"[TCP] Invalid payload length: {payloadLength}");
                break;
            }

            // Читаем JSON-payload
            totalRead = 0;
            while ((read = await stream.ReadAsync(payloadBuffer.AsMemory(totalRead, payloadLength - totalRead), ct)) > 0)
            {
                totalRead += read;
                if (totalRead == payloadLength) break;
            }

            if (read == 0 || totalRead < payloadLength || ct.IsCancellationRequested) break;

            // Парсим из Span (Срез в GO)
            var evt = System.Text.Json.JsonSerializer.Deserialize<CoordinateEvent>(
                payloadBuffer.AsSpan(0, payloadLength));

            await writer.WriteAsync(evt, ct); // in evt только для 64-128 байт
            Interlocked.Increment(ref producedCount);
        }
    }
    catch (OperationCanceledException) { Console.WriteLine("[TCP] Client handler cancelled."); }
    catch (IOException) { Console.WriteLine("[TCP] Client connection broken."); }
    catch (Exception ex) { Console.WriteLine($"[TCP] Unexpected Error:{ex.Message}"); }
    finally
    {
        waitGroup.Done();

        client.Close();
        Console.WriteLine($"[TCP] Client disconnected");
    }
}


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
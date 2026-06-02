using LiveObjectTracker.Service.Models;
using System.Threading.Channels;

Console.WriteLine("=== Start ===");

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

Console.WriteLine("=== Live Object Tracker ===");
Console.WriteLine($"Config: port={TcpPort}, capacity={ChannelCapacity}, workers={WorkersCount}");
Console.WriteLine($"Channel created: {channel.GetType().Name}");
Console.WriteLine($"Channel is bounded: {channel.Reader.CanCount}");

// TODO 1.3: Worker pool (TaskCreationOptions.LongRunning)
// TODO 1.4: Generator-client (~10k RPS)
// TODO 1.5: Console metrics (produced/consumed RPS, pending, cache size)
// TODO 2.1-2.2: PositionsCache (ReaderWriterLockSlim)
// TODO 3.3-3.4: Batch writers (Postgres COPY + ClickHouse)

Console.ReadLine();
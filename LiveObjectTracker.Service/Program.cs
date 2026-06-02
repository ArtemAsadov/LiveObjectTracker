using LiveObjectTracker.Service.Models;

Console.WriteLine("=== Live Object Tracker ===");

// TODO 1.2: TCP-сервер + BoundedChannel (10_000, Wait mode)
// TODO 1.3: Worker pool (TaskCreationOptions.LongRunning)
// TODO 1.4: Generator-client (~10k RPS)
// TODO 1.5: Console metrics (produced/consumed RPS, pending, cache size)
// TODO 2.1-2.2: PositionsCache (ReaderWriterLockSlim)
// TODO 3.3-3.4: Batch writers (Postgres COPY + ClickHouse)

// Smoke-test модели
var sample = new CoordinateEvent(
    ObjectId: 1,
    X: 10.5f,
    Y: 20.3f,
    Z: 5.7f,
    Timestamp: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

Console.WriteLine($"Sample event created: {sample}");
Console.WriteLine("Step 1.1 complete. Ready for 1.2 (TCP + Channel).");

Console.ReadLine();
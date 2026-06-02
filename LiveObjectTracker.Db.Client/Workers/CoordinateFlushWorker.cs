using LiveObjectTracker.Db.Entity;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Threading.Channels;

namespace LiveObjectTracker.Db.Client.Workers;

public sealed class CoordinateFlushWorker : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ChannelReader<CoordinateEntity> _reader;
    private readonly int _batchSize;

    public CoordinateFlushWorker(
        IServiceProvider serviceProvider,
        Channel<CoordinateEntity> channel,
        int batchSize = 1000)
    {
        _serviceProvider = serviceProvider;
        _reader = channel.Reader;
        _batchSize = batchSize;
    }
    
    //ADO.NET вроде быстрее хз, пока так
    protected override async Task ExecuteAsync(CancellationToken st = default)
    {
        var batch = new List<CoordinateEntity>(_batchSize);

        // Ждем данные на канале
        while (await _reader.WaitToReadAsync(st))
        {
            batch.Clear();

            while(batch.Count < _batchSize && _reader.TryRead(out var entity))
            {
                batch.Add(entity);
            }

            if (batch.Count == 0) continue;

            // СУПЕР ВАЖНО ТОЛЬКО СКОУП, не потоко безопасен
            using var scope = _serviceProvider.CreateScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            //EF Core
            await dbContext.Coordinates.AddRangeAsync(batch, st);
            await dbContext.SaveChangesAsync(st);
        }
    }
}

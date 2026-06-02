using LiveObjectTracker.DomainModel.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LiveObjectTracker.Service;

public sealed class PositionsCache : IDisposable
{
    private const long TtlMs = 30_000; // 30 секунд
    private const int CleanupIntervalsMs = 5_000; //Чистим каждые 5 секунд

    private readonly Dictionary<ulong, CoordinateEvent> _data = new(capacity: 1_000_000);
    private readonly ReaderWriterLockSlim _lock = new(LockRecursionPolicy.NoRecursion);


    private readonly Task _cleanupTask;
    private readonly CancellationTokenSource _cts;

    public PositionsCache(CancellationToken externalCt)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(externalCt);
        _cleanupTask = Task.Run(CleanupLoopAsync, _cts.Token);
    }

    public void Set(CoordinateEvent evt)
    {
        _lock.EnterWriteLock();
        try
        {
            _data.TryAdd(evt.ObjectId, evt);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public bool TryGet(ulong objectId, out CoordinateEvent evt)
    {
        _lock.EnterReadLock();
        try
        {
            if (_data.TryGetValue(objectId, out evt))
            {
                var age = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - evt.Timestamp;
                if (age <= TtlMs) return true;
            }

            evt = default(CoordinateEvent);
            return false;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public IReadOnlyCollection<CoordinateEvent> GetAll()
    {
        _lock.EnterReadLock();

        try
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            return _data.Values.Where(e => now - e.Timestamp <= TtlMs).ToArray();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public int Count
    {
        get
        {
            _lock.EnterReadLock();
            try
            {
                var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                return _data.Values.Count(e => now - e.Timestamp <= TtlMs);

            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }

    private async Task CleanupLoopAsync()
    {
        try
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                await Task.Delay(CleanupIntervalsMs, _cts.Token);
                Console.WriteLine("[Cleanup] ...");
                Cleanup();
            }
        }
        catch (OperationCanceledException)
        {
            // Нормальное завершение
        }
    }

    private void Cleanup()
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var cutoff = now - TtlMs;

        _lock.EnterWriteLock();
        try
        {
            var keysToRemove = _data
                .Where(kvp => kvp.Value.Timestamp < cutoff)
                .Select(kvp => kvp.Key)
                .ToArray();

            foreach (var key in keysToRemove)
            {
                _data.Remove(key);
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void Dispose()
    {
        _cts.Cancel();
        try { _cleanupTask.Wait(TimeSpan.FromSeconds(1)); } catch (AggregateException) { } // отмена прилетит как AggregateException, дадим секунжу на завершение

        _cts.Dispose();
        _lock.Dispose();
    }
}
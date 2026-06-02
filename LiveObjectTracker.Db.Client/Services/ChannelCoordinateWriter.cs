using LiveObjectTracker.Db.Entity;
using LiveObjectTracker.DomainModel.Models;
using System.Threading.Channels;

namespace LiveObjectTracker.Db.Client.Services;

public interface ICoordinateWriter
{
    ValueTask WriteAsync(CoordinateEvent evt, CancellationToken ct = default);
}

public class ChannelCoordinateWriter : ICoordinateWriter
{
    private ChannelWriter<CoordinateEntity> _writer;

    public ChannelCoordinateWriter(Channel<CoordinateEntity> channel)
    {
        _writer = channel.Writer;
    }

    public ValueTask WriteAsync(CoordinateEvent evt, CancellationToken ct = default)
    {
        var entity = new CoordinateEntity(
            ObjectId: (long)evt.ObjectId,
            X: evt.X,
            Y: evt.Y,
            Z: evt.Z,
            Timestamp: evt.Timestamp);

        return _writer.WriteAsync(entity, ct);
    }
}

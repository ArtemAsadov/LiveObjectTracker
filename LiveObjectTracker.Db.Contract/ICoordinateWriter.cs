using LiveObjectTracker.DomainModel.Models;

namespace LiveObjectTracker.Db.Contract;

internal interface ICoordinateWriter
{
    ValueTask WriteAsync(CoordinateEvent evt, CancellationToken ct);
}

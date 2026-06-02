using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LiveObjectTracker.DomainModel.Models;

public readonly record struct CoordinateEvent(
    ulong ObjectId,
    float X,
    float Y,
    float Z,
    long Timestamp);
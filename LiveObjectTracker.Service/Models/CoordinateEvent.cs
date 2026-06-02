using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LiveObjectTracker.Service.Models;

public readonly record struct CoordinateEvent(
    ulong ObjectId,
    float X,
    float Y,
    float Z,
    long Timestamp);
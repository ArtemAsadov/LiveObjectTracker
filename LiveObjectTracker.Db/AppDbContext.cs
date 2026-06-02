using LiveObjectTracker.Db.Entity;
using Microsoft.EntityFrameworkCore;

namespace LiveObjectTracker.Db;

public class AppDbContext: DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options)
    {

    }

    public DbSet<CoordinateEntity> Coordinates { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<CoordinateEntity>(entity =>
        {
            entity.HasKey(e => e.ObjectId);
            entity.Property(e => e.ObjectId).ValueGeneratedNever();
        });
    }
}

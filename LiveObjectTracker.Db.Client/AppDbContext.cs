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
            // Таблица в lowercase
            entity.ToTable("coordinates");

            // Маппинг колонок на snake_case
            entity.HasKey(e => e.ObjectId);
            entity.Property(e => e.ObjectId)
                .HasColumnName("object_id")
                .ValueGeneratedNever();

            entity.Property(e => e.X).HasColumnName("x");
            entity.Property(e => e.Y).HasColumnName("y");
            entity.Property(e => e.Z).HasColumnName("z");
            entity.Property(e => e.Timestamp).HasColumnName("timestamp");
        });
    }
}

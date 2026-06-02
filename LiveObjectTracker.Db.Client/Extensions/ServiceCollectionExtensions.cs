using LiveObjectTracker.Db.Client.Workers;
using LiveObjectTracker.Db.Client.Services;
using LiveObjectTracker.Db.Entity;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using System.Threading.Channels;

namespace LiveObjectTracker.Db.Client.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddCoordinateDb(
        this IServiceCollection services,
        string connectionString)
    {

        //Канал для асинхронной записи
        var channel = Channel.CreateUnbounded<CoordinateEntity>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

        services.AddSingleton(channel);

        //EF Core
        services.AddDbContext<AppDbContext>(options =>
        {
            options.UseNpgsql(connectionString);
            options.UseQueryTrackingBehavior(QueryTrackingBehavior.NoTracking);
        });

        //Writer
        services.AddSingleton<ChannelCoordinateWriter>(sp =>
        {
            var ctx = sp.GetRequiredService<Channel<CoordinateEntity>>();
            return new ChannelCoordinateWriter(ctx);
        });

        // Writer
        services.AddSingleton<ICoordinateWriter, ChannelCoordinateWriter>();

        //Sender
        services.AddHostedService<CoordinateFlushWorker>();

        return services;
    }
}

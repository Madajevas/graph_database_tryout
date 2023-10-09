
using deniszykov.CommandLine;

using GraphDatabaseTryout.Data;
using GraphDatabaseTryout.Migrations;

using Microsoft.Extensions.DependencyInjection;

using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

using System.Diagnostics;

class Program
{
    public static void Main(string[] arguments)
    {
        CommandLine
            .CreateFromArguments(arguments)
            .Use<Program>()
            .Run();
    }

    public static int Migrate(bool down = false)
    {
        Migrator.Migrate(down);

        return 0;
    }

    public static async Task<int> Load(string path)
    {
        using var meterProvider = Sdk.CreateMeterProviderBuilder()
            .AddConsoleExporter()
            .AddMeter("Test.Metrics")
            .Build();
        using var traceProvider = Sdk.CreateTracerProviderBuilder()
            .AddConsoleExporter()
            .AddSource("Test.Performance")
            .Build();

        var services = new ServiceCollection();
        services.AddDataLoading();
        services.AddMemoryCache();

        using var provider = services.BuildServiceProvider();
        using var activitySource = new ActivitySource("Test.Performance");
        using var _ = activitySource.StartActivity("Load");
        await using var scope = provider.CreateAsyncScope();

        await scope.ServiceProvider.GetRequiredService<DataLoader>().LoadAsync(path);

        return 0;
    }
}

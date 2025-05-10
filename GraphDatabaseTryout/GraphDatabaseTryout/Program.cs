
using deniszykov.CommandLine;

using GraphDatabaseTryout.Data;
using GraphDatabaseTryout.Migrations;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.ObjectPool;

using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

using System.Diagnostics;
using System.IO.Compression;

class Program
{
    public static void Main(string[] arguments)
    {
        CommandLine
            .CreateFromArguments(arguments)
            .Use<Program>()
            .Run();
    }

    public static async Task<int> Download()
    {
        var url = "https://datasets.imdbws.com/title.basics.tsv.gz";
        using var client = new HttpClient();
        using var response = await client.GetAsync(url);
        using var stream = await response.Content.ReadAsStreamAsync();
        using var gzipStream = new GZipStream(stream, CompressionMode.Decompress);
        using var fileStream = File.Create(Path.Combine("../../data", "title.basics.tsv"));
        await gzipStream.CopyToAsync(fileStream);

        return 0;
    }

    public static int Migrate(bool down = false)
    {
        Migrator.Migrate(down);

        return 0;
    }

    public static async Task<int> Load(string path = "../../data")
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
        services.TryAddSingleton<ObjectPoolProvider, DefaultObjectPoolProvider>();
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

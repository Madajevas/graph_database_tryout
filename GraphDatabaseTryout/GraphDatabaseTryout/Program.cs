
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
        async Task Download(string file)
        {
            var url = $"https://datasets.imdbws.com/{file}.gz";
            using var client = new HttpClient();
            using var response = await client.GetAsync(url);
            using var stream = await response.Content.ReadAsStreamAsync();
            using var gzipStream = new GZipStream(stream, CompressionMode.Decompress);
            using var fileStream = File.Create(Path.Combine(@"..\..\..\data", file));
            await gzipStream.CopyToAsync(fileStream);
        }

        // await Download("title.basics.tsv");
        // await Download("name.basics.tsv");
        // await Download("title.crew.tsv");
        await Download("title.principals.tsv");
        // await Download("title.episode.tsv");
        // await Download("title.ratings.tsv");
        // await Download("title.akas.tsv");


        return 0;
    }

    public static int Migrate(bool down = false)
    {
        Migrator.Migrate(down);

        return 0;
    }

    public static async Task<int> Load(string path = @"..\..\..\data")
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

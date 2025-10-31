using GraphDatabaseTryout.Data;
using GraphDatabaseTryout.Migrations;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.ObjectPool;

using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

using System.CommandLine;
using System.Diagnostics;
using System.IO.Compression;

var rootCommand = new RootCommand("Graph Database checkout");

var downloadDestinationArgument = new Argument<DirectoryInfo>("destination");
var downloadCommand = new Command("download", "Downloads data files");
downloadCommand.Arguments.Add(downloadDestinationArgument);
downloadCommand.SetAction(async parseResult =>
{
    var directory = parseResult.GetRequiredValue(downloadDestinationArgument);
    if (!directory.Exists)
    {
        throw new DirectoryNotFoundException();
    }

    string[] files = ["title.basics.tsv", "name.basics.tsv", "title.crew.tsv", "title.principals.tsv", "title.episode.tsv", "title.ratings.tsv", "title.akas.tsv"];
    using var client = new HttpClient();

    foreach (var file in files)
    {
        var url = $"https://datasets.imdbws.com/{file}.gz";
        using var response = await client.GetAsync(url);
        using var stream = await response.Content.ReadAsStreamAsync();
        using var gzipStream = new GZipStream(stream, CompressionMode.Decompress);
        using var fileStream = File.Create(Path.Combine(directory.FullName, file));
        await gzipStream.CopyToAsync(fileStream);
    }
});
rootCommand.Subcommands.Add(downloadCommand);

var connectionStringArgument = new Argument<string>("connectionString");

var migrateCommand = new Command("migrate", "Prepares database");
migrateCommand.Arguments.Add(connectionStringArgument);
migrateCommand.SetAction(parseResult =>
{
    var connectionString = parseResult.GetRequiredValue(connectionStringArgument);
    Migrator.Migrate(connectionString, false);
});
rootCommand.Subcommands.Add(migrateCommand);

var sourceFilesDirectoryArgument = new Argument<DirectoryInfo>("source");
var loadCommand = new Command("load", "Loads data into database");
loadCommand.Arguments.Add(sourceFilesDirectoryArgument);
loadCommand.Arguments.Add(connectionStringArgument);
loadCommand.SetAction(async parseResult =>
{
    var source = parseResult.GetRequiredValue(sourceFilesDirectoryArgument);
    var connectionString = parseResult.GetRequiredValue(connectionStringArgument);

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
    services.AddDataLoading(connectionString);
    services.AddHybridCache();

    using var activitySource = new ActivitySource("Test.Performance");
    services.AddSingleton(activitySource);

    using var provider = services.BuildServiceProvider();
    await using var scope = provider.CreateAsyncScope();

    using var _ = activitySource.StartActivity("Load");

    await scope.ServiceProvider.GetRequiredService<DataLoader>().LoadAsync(source.FullName);
});
rootCommand.Subcommands.Add(loadCommand);


await rootCommand.Parse(args).InvokeAsync();

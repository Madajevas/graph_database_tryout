
using deniszykov.CommandLine;

using GraphDatabaseTryout.Data;
using GraphDatabaseTryout.Migrations;
using GraphDatabaseTryout.Performance;

using Microsoft.Extensions.DependencyInjection;

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
        var services = new ServiceCollection();
        services.AddDataLoading();
        services.AddMemoryCache();

        using var provider = services.BuildServiceProvider();
        using var _ = new PerformanceCounter(nameof(Load));
        await using var scope = provider.CreateAsyncScope();

        await scope.ServiceProvider.GetRequiredService<DataLoader>().LoadAsync(path);

        return 0;
    }
}

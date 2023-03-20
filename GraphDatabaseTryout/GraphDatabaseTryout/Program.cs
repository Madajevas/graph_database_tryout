
using deniszykov.CommandLine;

using GraphDatabaseTryout.Data;
using GraphDatabaseTryout.Migrations;

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

    public static int Load(string path)
    {
        var services = new ServiceCollection();
        services.AddDataLoading();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        scope.ServiceProvider.GetRequiredService<DataLoader>().Load(path);

        return 0;
    }
}

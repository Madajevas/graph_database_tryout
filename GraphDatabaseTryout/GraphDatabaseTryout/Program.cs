
using deniszykov.CommandLine;

using FluentMigrator.Runner;

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
        var connectionString = Environment.GetEnvironmentVariable("ConnectionString:Graph")
            ?? throw new ArgumentException("Connection string missing");

        using var services = new ServiceCollection()
            .AddFluentMigratorCore()
            .ConfigureRunner(rb => rb
                .AddSqlServer()
                .WithGlobalConnectionString(connectionString)
                .ScanIn(typeof(Program).Assembly).For.Migrations())
            .AddLogging(lb => lb.AddFluentMigratorConsole())
            .BuildServiceProvider(false);
        var scope = services.CreateScope();
        var runner = scope.ServiceProvider.GetRequiredService<IMigrationRunner>();

        if (down)
        {
            runner.MigrateDown(0);
        }
        else
        {
            runner.MigrateUp();
        }

        return 0;
    }
}

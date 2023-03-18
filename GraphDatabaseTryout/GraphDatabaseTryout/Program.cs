
using deniszykov.CommandLine;

using GraphDatabaseTryout.Data;
using GraphDatabaseTryout.Migrations;

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
        DataLoader.Load(path);

        return 0;
    }
}

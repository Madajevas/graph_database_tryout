using FluentMigrator;

namespace GraphDatabaseTryout.Migrations.Graph
{
    [Migration(202303162102)]
    public class Migration202303162102CreateSchema : Migration
    {
        public override void Up()
        {
            Execute.Sql("""
                        CREATE TABLE genre (
                            name VARCHAR(100)
                        ) AS NODE
                        """);
            Execute.Sql("""
                        CREATE TABLE movie (
                            ID INTEGER PRIMARY KEY,
                            name VARCHAR(300),
                            year SMALLINT NULL,
                            length SMALLINT NULL
                        )  AS NODE
                        """);
            Execute.Sql("""
                        CREATE TABLE person (
                            ID INTEGER PRIMARY KEY,
                            name VARCHAR(100),
                            gender VARCHAR(10) NULL,
                            birth_date VARCHAR(10) NULL
                        )  AS NODE
                        """);

            Execute.Sql("CREATE TABLE is_of AS EDGE");
            Execute.Sql("""CREATE TABLE starred_in ("as" VARCHAR(50)) AS EDGE""");
            Execute.Sql("CREATE TABLE directed AS EDGE");
        }

        public override void Down() { }
    }
}

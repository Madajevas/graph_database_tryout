using Dapper;

using System.Data;

namespace GraphDatabaseTryout.Data.Repositories
{
    // TODO: does this makes sence with graph api?
    internal class GenresRepository
    {
        // upsert is not really necessary, but since all this is to learn, why not
        private const string genreUpsertSql = """
                    MERGE INTO genre old
                    USING (VALUES (@genre)) AS new (name)
                    ON old.name = new.name
                    WHEN MATCHED THEN UPDATE SET old.name = new.name                -- this line is not needed
                    WHEN NOT MATCHED BY TARGET THEN INSERT (name) VALUES (name)
                    OUTPUT inserted.$node_id;
                    """;

        private readonly IDbConnection connection;

        public GenresRepository(IDbConnection connection)
        {
            this.connection = connection;
        }

        // TODO: make async
        // TODO: strongly type return value?
        public string SaveGenre(string genre) => connection.ExecuteScalar<string>(genreUpsertSql, new { genre });
    }
}

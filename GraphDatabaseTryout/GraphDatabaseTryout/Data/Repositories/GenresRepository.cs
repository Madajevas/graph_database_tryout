using Dapper;

using Microsoft.Extensions.Caching.Memory;

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
                    WHEN MATCHED THEN UPDATE set old.name = new.name
                    WHEN NOT MATCHED BY TARGET THEN INSERT (name) VALUES (name)
                    OUTPUT inserted.$node_id;
                    """;

        private readonly IDbConnection connection;
        private readonly IMemoryCache cache;

        public GenresRepository(IDbConnection connection, IMemoryCache cache)
        {
            this.connection = connection;
            this.cache = cache;
        }

        // TODO: make async
        // TODO: strongly type return value?
        public string SaveGenre(string genre)
        {
            return cache.GetOrCreate(genre, _ => connection.ExecuteScalar<string>(genreUpsertSql, new { genre }))!;
        }
    }
}

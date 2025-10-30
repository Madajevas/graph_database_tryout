using Dapper;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Caching.Hybrid;

namespace GraphDatabaseTryout.Data.Repositories
{
    // TODO: does this makes sense with graph api?
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

        private readonly SqlConnection connection;
        private readonly HybridCache cache;

        public GenresRepository(SqlConnection connection, HybridCache cache)
        {
            this.connection = connection;
            this.connection.Open();
            this.cache = cache;
        }

        // TODO: strongly type return value?
        public ValueTask<string> SaveGenreAsync(string genre)
        {
            return cache.GetOrCreateAsync<string>(genre, async _ => await connection.ExecuteScalarAsync<string>(genreUpsertSql, new { genre }));
        }
    }
}

using Dapper;

using GraphDatabaseTryout.Data.Models;

using System.Data;
using System.Text;

namespace GraphDatabaseTryout.Data.Repositories
{
    internal class MoviesRepository
    {
        private const string insertSql = """
            INSERT INTO movie (ID, name, year, length) OUTPUT Inserted.$node_id
            VALUES (@TConst, @Name, @Year, @Length)
            """;

        private const int BatchSize = 20;
        private string insertBatchSql = GetBulkInsertQuery(BatchSize);
        // private const string insert10Sql = """
        //     INSERT INTO movie (ID, name, year, length) OUTPUT Inserted.$node_id
        //     VALUES (@TConst0, @Name0, @Year0, @Length0), (@TConst1, @Name1, @Year1, @Length1), (@TConst2, @Name2, @Year2, @Length2), (@TConst3, @Name3, @Year3, @Length3), (@TConst4, @Name4, @Year4, @Length4), (@TConst5, @Name5, @Year5, @Length5), (@TConst6, @Name6, @Year6, @Length6), (@TConst7, @Name7, @Year7, @Length7), (@TConst8, @Name8, @Year8, @Length8), (@TConst9, @Name9, @Year9, @Length9)
        //     """;

        private readonly IDbConnection connection;

        public MoviesRepository(IDbConnection connection)
        {
            this.connection = connection;

            Dapper.SqlMapper.AddTypeMap(typeof(uint?), DbType.Int32);
            Dapper.SqlMapper.AddTypeMap(typeof(uint), DbType.Int32);
        }

        public Task<string> SaveAsync(Movie movie)
        {
            return connection.ExecuteScalarAsync<string>(insertSql, movie);
        }

        public async IAsyncEnumerable<string> SaveAsync(IEnumerable<Movie> movies)
        {
            foreach (var movieChunk in movies.Chunk(BatchSize))
            {
                var insertSql = movieChunk.Length == BatchSize ? insertBatchSql : GetBulkInsertQuery(movieChunk.Length);

                foreach (var id in await connection.QueryAsync<string>(insertSql, GetParameters(movieChunk)))
                {
                    yield return id;
                }
            }
        }

        private static string GetBulkInsertQuery(int batchSize)
        {
            var builder = new StringBuilder("INSERT INTO movie (ID, name, year, length) OUTPUT Inserted.$node_id VALUES");
            for (var i = 0; i < batchSize; i++)
            {
                builder.AppendFormat("(@TConst{0}, @Name{0}, @Year{0}, @Length{0}),", i);
            }

            builder.Length -= 1;

            return builder.ToString();
        }

        private static DynamicParameters GetParameters(IEnumerable<Movie> movies)
        {
            var i = 0;
            var parameter = new DynamicParameters();

            foreach (var movie in movies)
            {
                parameter.Add($"@TConst{i}", movie.TConst);
                parameter.Add($"@Name{i}", movie.Name);
                parameter.Add($"@Year{i}", (int?)movie.Year, DbType.Int32);
                parameter.Add($"@Length{i}", (int?)movie.Length, DbType.Int32);
                i++;
            }

            return parameter;
        }
    }
}

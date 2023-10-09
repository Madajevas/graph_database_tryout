using Dapper;

using GraphDatabaseTryout.Data.Models;

using Microsoft.Extensions.ObjectPool;

using System.Data;
using System.Diagnostics.Metrics;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks.Dataflow;

namespace GraphDatabaseTryout.Data.Repositories
{
    internal class MoviesRepository : IAsyncDisposable
    {
        private static Meter meter = new Meter("Test.Metrics");
        private static Counter<int> moviesCounter = meter.CreateCounter<int>("Movies.Inserted");


        private const string insertSql = """
            INSERT INTO movie (ID, name, year, length) OUTPUT Inserted.$node_id
            VALUES (@TConst, @Name, @Year, @Length)
            """;

        private const int BatchSize = 20;
        private string insertBatchSql = GetBulkInsertQuery(BatchSize);
        private ActionBlock<Movie> insertBlock;
        private readonly IDbConnection connection;
        private readonly GenresRepository genresRepository;
        private readonly ObjectPool<IDbConnection> connectionPool;

        public MoviesRepository(IDbConnection connection, GenresRepository genresRepository, ObjectPool<IDbConnection> connectionPool)
        {
            this.connection = connection;
            this.genresRepository = genresRepository;
            this.connectionPool = connectionPool;

            var options = new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 10,
            };
            insertBlock = new ActionBlock<Movie>(SaveAsyncInternal, options);

            Dapper.SqlMapper.AddTypeMap(typeof(uint?), DbType.Int32);
            Dapper.SqlMapper.AddTypeMap(typeof(uint), DbType.Int32);
        }

        public Task SaveAsync(Movie movie)
        {
            return insertBlock.SendAsync(movie);
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

        public async ValueTask DisposeAsync()
        {
            insertBlock.Complete();
            await insertBlock.Completion;
        }

        private async Task SaveAsyncInternal(Movie movie)
        {
            var connection = connectionPool.Get();

            var movieNodeId = await connection.ExecuteScalarAsync<string>(insertSql, movie);
            var insertOneSql = "INSERT INTO is_of VALUES (@MovieId, @GenreId)";
            foreach (var genre in movie.Genres)
            {
                var genreId = await genresRepository.SaveGenreAsync(genre);
                await connection.ExecuteAsync(insertOneSql, new { MovieId = movieNodeId, GenreId = genreId });
            }

            connectionPool.Return(connection);

            moviesCounter.Add(1);
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

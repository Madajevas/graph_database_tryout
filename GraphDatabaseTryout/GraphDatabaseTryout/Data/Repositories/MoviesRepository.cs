using Dapper;

using GraphDatabaseTryout.Data.Models;

using Microsoft.Extensions.DependencyInjection;

using System.Data;
using System.Diagnostics.Metrics;
using System.Text;
using System.Threading.Channels;

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

        private readonly IDbConnection connection;
        private readonly GenresRepository genresRepository;
        private readonly IServiceProvider serviceProvider;
        private readonly Channel<Movie> insertChannel;
        private readonly Task[] insertTasks;

        public MoviesRepository(IDbConnection connection, GenresRepository genresRepository, IServiceProvider serviceProvider)
        {
            this.connection = connection;
            this.genresRepository = genresRepository;
            this.serviceProvider = serviceProvider;
            insertChannel = Channel.CreateUnbounded<Movie>();
            insertTasks = new[] {
                StartInsertLoop(), StartInsertLoop(), StartInsertLoop(), StartInsertLoop(), StartInsertLoop(),
                StartInsertLoop(), StartInsertLoop(), StartInsertLoop(), StartInsertLoop(), StartInsertLoop(),
            }; 

            Dapper.SqlMapper.AddTypeMap(typeof(uint?), DbType.Int32);
            Dapper.SqlMapper.AddTypeMap(typeof(uint), DbType.Int32);
        }

        public ValueTask SaveAsync(Movie movie)
        {
            return insertChannel.Writer.WriteAsync(movie);
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
            insertChannel.Writer.Complete();
            await Task.WhenAll(insertTasks);
        }

        private async Task StartInsertLoop()
        {
            var connection = serviceProvider.GetService<IDbConnection>();
            await foreach (var movie in insertChannel.Reader.ReadAllAsync())
            {
                var movieNodeId = await connection.ExecuteScalarAsync<string>(insertSql, movie);

                var insertOneSql = "INSERT INTO is_of VALUES (@MovieId, @GenreId)";
                foreach (var genre in movie.Genres)
                {
                    var genreId = await genresRepository.SaveGenreAsync(genre);
                    await connection.ExecuteAsync(insertOneSql, new { MovieId = movieNodeId, GenreId = genreId });
                }

                moviesCounter.Add(1);
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

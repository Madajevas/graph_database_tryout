using Dapper;

using GraphDatabaseTryout.Data.Models;

using Microsoft.Extensions.ObjectPool;

using System.Data;
using System.Diagnostics.Metrics;

namespace GraphDatabaseTryout.Data.Repositories
{
    internal class MoviesRepository
    {
        private static Meter meter = new Meter("Test.Metrics");
        private static Counter<int> moviesCounter = meter.CreateCounter<int>("Movies.Inserted");

        private const string insertSql = """
            INSERT INTO movie (ID, name, year, length) OUTPUT Inserted.$node_id
            VALUES (@Id, @Name, @Year, @Length)
            """;

        private readonly GenresRepository genresRepository;
        private readonly ObjectPool<IDbConnection> connectionPool;

        public MoviesRepository(GenresRepository genresRepository, ObjectPool<IDbConnection> connectionPool)
        {
            this.genresRepository = genresRepository;
            this.connectionPool = connectionPool;

            Dapper.SqlMapper.AddTypeMap(typeof(uint?), DbType.Int32);
            Dapper.SqlMapper.AddTypeMap(typeof(uint), DbType.Int32);
        }

        public async Task SaveAsync(Movie movie)
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
    }
}

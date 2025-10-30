using Dapper;

using GraphDatabaseTryout.Data.Models;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.ObjectPool;

using System;
using System.Data;
using System.Diagnostics.Metrics;

using static System.Runtime.InteropServices.JavaScript.JSType;

namespace GraphDatabaseTryout.Data.Repositories
{
    internal class MoviesRepository
    {
        private static Meter meter = new Meter("Test.Metrics");
        private static Counter<int> moviesCounter = meter.CreateCounter<int>("Movies.Inserted");

        private const string outputSql = """
            INSERT INTO movie (ID, name, year, length)
            OUTPUT Inserted.$node_id
            SELECT ID, name, year, length FROM #movie
            """;

        private readonly GenresRepository genresRepository;
        private readonly IServiceProvider provider;

        public MoviesRepository(GenresRepository genresRepository, IServiceProvider provider)
        {
            this.genresRepository = genresRepository;
            this.provider = provider;
            Dapper.SqlMapper.AddTypeMap(typeof(uint?), DbType.Int32);
            Dapper.SqlMapper.AddTypeMap(typeof(uint), DbType.Int32);
        }

        public async Task SaveAsync(IReadOnlyCollection<Movie> movies)
        {
            using var connection = provider.GetRequiredService<SqlConnection>();
            await connection.OpenAsync();
            using var transaction = connection.BeginTransaction();

            await connection.ExecuteAsync("""
                create table #movie
                (
                    [ID] int not NULL,
                    [name] varchar(1000) NULL,
                    [year] smallint NULL,
                    [length] int NULL
                ); 
                """, transaction: transaction);
            var sqlBulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction)
            {
                DestinationTableName = "#movie",
                BatchSize = 100,
            };
            sqlBulkCopy.ColumnMappings.Add(nameof(Movie.Id), "ID");
            sqlBulkCopy.ColumnMappings.Add(nameof(Movie.Name), "name");
            sqlBulkCopy.ColumnMappings.Add(nameof(Movie.Year), "year");
            sqlBulkCopy.ColumnMappings.Add(nameof(Movie.Length), "length");

            var table = new DataTable();
            table.Columns.Add(nameof(Movie.Id), typeof(int));
            table.Columns.Add(nameof(Movie.Name), typeof(string));
            table.Columns.Add(nameof(Movie.Year), typeof(uint));
            table.Columns.Add(nameof(Movie.Length), typeof(uint));

            foreach (var movie in movies)
            {
                var row = table.NewRow();
                row[nameof(Movie.Id)] = movie.Id;
                row[nameof(Movie.Name)] = movie.Name;
                row[nameof(Movie.Year)] = movie.Year.HasValue ? movie.Year.Value : DBNull.Value;
                row[nameof(Movie.Length)] = movie.Length.HasValue ? movie.Length.Value : DBNull.Value;
                table.Rows.Add(row);
            }

            await sqlBulkCopy.WriteToServerAsync(table);
            var movieNodeIds = await connection.QueryAsync<string>(outputSql, transaction: transaction);

            var movieGenreBulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction)
            {
                DestinationTableName = "is_of",
                BatchSize = 100,
            };
            var table2 = new DataTable();
            table2.Columns.Add("$from_id", typeof(string));
            table2.Columns.Add("$to_id", typeof(string));
            foreach (var (movieId, movie) in movieNodeIds.Zip(movies))
            {
                foreach (var genre in movie.Genres)
                {
                    var genreId = await genresRepository.SaveGenreAsync(genre);
                    var row = table2.NewRow();
                    row["$from_id"] = movieId;
                    row["$to_id"] = genreId;
                }
            }
            await movieGenreBulkCopy.WriteToServerAsync(table2);


            transaction.Commit();
            moviesCounter.Add(movies.Count);
        }
    }
}

using Dapper;

using GraphDatabaseTryout.Data.Models;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;

using System.Data;
using System.Diagnostics.Metrics;
using System.Threading.Tasks.Dataflow;

namespace GraphDatabaseTryout.Data.Repositories;

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

    public async Task SaveAsync(IEnumerable<Movie> movies)
    {
        var blockOptions = new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 16, BoundedCapacity = 5 };

        var batchBock = new BatchBlock<Movie>(100, new GroupingDataflowBlockOptions { BoundedCapacity = 1000 });

        var insertMoviesBlock = new TransformBlock<IReadOnlyCollection<Movie>, IReadOnlyCollection<(string MovieId, Movie Movie)>>(GetInsertMovies, blockOptions);
        batchBock.LinkTo(insertMoviesBlock);
        var insertGenresBlock = new TransformBlock<IReadOnlyCollection<(string MovieId, Movie Movie)>, int>(GetInsertGenres, blockOptions);
        insertMoviesBlock.LinkTo(insertGenresBlock);
        var reportBlock = new ActionBlock<int>(moviesCounter.Add);
        insertGenresBlock.LinkTo(reportBlock);

        foreach (var movie in movies)
        {
            await batchBock.SendAsync(movie);
        }
        batchBock.Complete();
        await reportBlock.Completion;
    }

    private async Task<int> GetInsertGenres(IReadOnlyCollection<(string MovieId, Movie Movie)> movies)
    {
        using var connection = provider.GetRequiredService<SqlConnection>();
        await connection.OpenAsync();
        using var transaction = connection.BeginTransaction();

        await connection.ExecuteAsync("""
            CREATE TABLE #movie_genre
            (
                [from_id] nvarchar(1000) NOT NULL,
                [to_id] nvarchar(1000) NOT NULL
            ); 
            """, transaction: transaction);

        var sqlBulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction)
        {
            DestinationTableName = "#movie_genre",
            BatchSize = 100,
        };

        var table = new DataTable();
        table.Columns.Add("from_id", typeof(string));
        table.Columns.Add("to_id", typeof(string));
        foreach (var (movieId, movie) in movies)
        {
            foreach (var genre in movie.Genres)
            {
                var genreId = await genresRepository.SaveGenreAsync(genre);
                var row = table.NewRow();
                row["from_id"] = movieId;
                row["to_id"] = genreId;
                table.Rows.Add(row);
            }
        }

        await sqlBulkCopy.WriteToServerAsync(table);

        string copySql = """
            INSERT INTO is_of
            SELECT from_id, to_id FROM #movie_genre
            """;
        await connection.ExecuteAsync(copySql, transaction: transaction);

        await transaction.CommitAsync();

        return movies.Count;
    }

    private async Task<IReadOnlyCollection<(string MovieId, Movie Movie)>> GetInsertMovies(IReadOnlyCollection<Movie> movies)
    {
        using var connection = provider.GetRequiredService<SqlConnection>();
        await connection.OpenAsync();
        using var transaction = connection.BeginTransaction();

        await connection.ExecuteAsync("""
            CREATE TABLE #movie
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

        await transaction.CommitAsync();

        return movieNodeIds.Zip(movies).ToArray();
    }
}

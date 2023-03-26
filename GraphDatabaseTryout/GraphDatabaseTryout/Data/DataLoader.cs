using CsvHelper;
using CsvHelper.Configuration;

using GraphDatabaseTryout.Data.Models;
using GraphDatabaseTryout.Data.Repositories;
using GraphDatabaseTryout.Performance;

using System.Diagnostics;
using System.Globalization;

namespace GraphDatabaseTryout.Data
{
    internal class DataLoader
    {
        private readonly GenresRepository genresRepository;
        private readonly MoviesRepository moviesRepository;

        public DataLoader(GenresRepository genresRepository, MoviesRepository moviesRepository)
        {
            this.genresRepository = genresRepository;
            this.moviesRepository = moviesRepository;
        }

        public Task LoadAsync(string path)
        {
            var movies = GetMovies(path);
            return SaveMoviesAsync(movies);
        }

        private static IEnumerable<Movie> GetMovies(string path)
        {
            var titleBasics = Path.Combine(path, "title.basics.tsv");

            var configuration = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = "\t",
                // Mode = CsvMode.Escape,
                TrimOptions = TrimOptions.Trim,

                BadDataFound = args => Console.WriteLine($"Bad row found: {args.RawRecord}"),
            };

            using (var reader = new StreamReader(titleBasics))
            using (var noQuotesSteam = new RemoveQuotesStream(reader))
            using (var csv = new CsvReader(noQuotesSteam, configuration))
            {
                csv.Context.RegisterClassMap<MovieMap>();
                foreach (var movie in csv.GetRecords<Movie>())
                {
                    yield return movie;
                }
            }
        }

        private async Task SaveMoviesAsync(IEnumerable<Movie> movies)
        {
            using var _ = new PerformanceCounter(nameof(SaveMoviesAsync));

            async IAsyncEnumerable<string> SaveGenres(string[] genres)
            {
                foreach (var genre in  genres)
                {
                    var nodeId = await genresRepository.SaveGenreAsync(genre);
                    Debug.Assert(nodeId != null);
                    yield return nodeId;
                }
            }

            foreach (var movie in movies.Take(100_000))
            {
                var genreNodeIds = await SaveGenres(movie.Genres).ToListAsync();
                var movieNodeId = await moviesRepository.SaveAsync(movie);
                Debug.Assert(movieNodeId != null);
            }
        }
    }
}

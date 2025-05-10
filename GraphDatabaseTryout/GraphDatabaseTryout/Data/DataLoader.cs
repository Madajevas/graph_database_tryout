using CsvHelper;
using CsvHelper.Configuration;

using GraphDatabaseTryout.Data.Models;
using GraphDatabaseTryout.Data.Repositories;

using System.Diagnostics;
using System.Globalization;

namespace GraphDatabaseTryout.Data
{
    internal class DataLoader
    {
        private readonly GenresRepository genresRepository;
        private readonly MoviesRepository moviesRepository;
        private readonly MovieToGenreEdgesRepository movieToGenreEdgesRepository;

        public DataLoader(GenresRepository genresRepository, MoviesRepository moviesRepository, MovieToGenreEdgesRepository movieToGenreEdgesRepository)
        {
            this.genresRepository = genresRepository;
            this.moviesRepository = moviesRepository;
            this.movieToGenreEdgesRepository = movieToGenreEdgesRepository;
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
            foreach (var fewMovies in movies.Take(100_000).Chunk(20))
            {
                var inserts = fewMovies.AsParallel().Select(moviesRepository.SaveAsync);
                await Task.WhenAll(inserts);
            }
        }
    }
}

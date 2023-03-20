using CsvHelper;
using CsvHelper.Configuration;

using GraphDatabaseTryout.Data.Models;
using GraphDatabaseTryout.Data.Repositories;

using System.Globalization;

namespace GraphDatabaseTryout.Data
{
    internal class DataLoader
    {
        private readonly GenresRepository genresRepository;

        public DataLoader(GenresRepository genresRepository)
        {
            this.genresRepository = genresRepository;
        }

        public void Load(string path)
        {
            var movies = GetMovies(path);
            SaveMovies(movies);
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

        private void SaveMovies(IEnumerable<Movie> movies)
        {
            IEnumerable<string> SaveGenres(string[] genres)
            {
                foreach (var genre in  genres)
                {
                    yield return genresRepository.SaveGenre(genre);
                }
            }

            foreach (var movie in movies)
            {
                var genreNodeIds = SaveGenres(movie.Genres).ToList();
            }
        }
    }
}

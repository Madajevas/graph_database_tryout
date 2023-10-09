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
            async IAsyncEnumerable<string> SaveGenres(string[] genres)
            {
                foreach (var genre in  genres)
                {
                    var nodeId = await genresRepository.SaveGenreAsync(genre);
                    Debug.Assert(nodeId != null);
                    yield return nodeId;
                }
            }

            async IAsyncEnumerable<(string, IEnumerable<string>)> GetAssociations(IList<string> movieNodeIds, IList<IAsyncEnumerable<string>> genreNodeIdsEnumerables)
            {
                for (var i = 0; i < movieNodeIds.Count; i++)
                {
                    var genderNodeIds = await genreNodeIdsEnumerables[i].ToListAsync();

                    yield return (movieNodeIds[i], genderNodeIds);
                }
            }

            foreach (var movie in movies.Take(100_00))
            {
                // var genresNodeIds = await SaveGenres(movie.Genres).ToListAsync();
                await moviesRepository.SaveAsync(movie);

                // await movieToGenreEdgesRepository.AssociateAsync(movie, genresNodeIds);
            }

            return;

            foreach (var movieChunk in movies.Take(100_000).Chunk(100))
            {
                var genresNodeIdsTasks = movieChunk.Select(movie => SaveGenres(movie.Genres)).ToList();
                var movieIdsTask = await moviesRepository.SaveAsync(movieChunk).ToListAsync();

                await movieToGenreEdgesRepository.AssociateAsync(await GetAssociations(movieIdsTask, genresNodeIdsTasks).ToListAsync());

                // this is getting out of hand.
                // I must be looking at this wrong

                // var genreNodeIds = await SaveGenres(movie.Genres).ToListAsync();
                // var movieNodeId = await moviesRepository.SaveAsync(movie);
                // Debug.Assert(movieNodeId != null);
            }
        }
    }
}

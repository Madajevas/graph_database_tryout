using CsvHelper;
using CsvHelper.Configuration;

using GraphDatabaseTryout.Data.Models;

using System.Globalization;

namespace GraphDatabaseTryout.Data
{
    internal class DataLoader
    {
        public static void Load(string path)
        {
            var movies = GetMovies(path).ToList();
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
    }
}

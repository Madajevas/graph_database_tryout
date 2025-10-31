using CsvHelper;
using CsvHelper.Configuration;

using GraphDatabaseTryout.Data.Models;
using GraphDatabaseTryout.Data.Repositories;

using System.Diagnostics;
using System.Globalization;

namespace GraphDatabaseTryout.Data
{
    internal class DataLoader(MoviesRepository moviesRepository, PersonsRepository personsRepository, JobsRepository jobsRepository, ActivitySource activitySource)
    {
        public async Task LoadAsync(string path)
        {
            // using (activitySource.StartActivity("Load Movies"))
            // {
            //     var movies = ParseFile<Movie, MovieMap>(Path.Combine(path, "title.basics.tsv"));
            //     await moviesRepository.SaveAsync(movies);
            // }

            using (activitySource.StartActivity("Load Persons"))
            {
                var persons = ParseFile<Person, PersonMap>(Path.Combine(path, "name.basics.tsv"));
                await personsRepository.SaveAsync(persons);
            }
            return;

            // var jobs = ParseFile<Job, JobMap>(Path.Combine(path, "title.principals.tsv"));
            // return SaveJobsAsync(jobs);
        }

        private static IEnumerable<T> ParseFile<T, TMap>(string filePath) where TMap : ClassMap<T>
        {
            var configuration = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = "\t",
                // Mode = CsvMode.Escape,
                TrimOptions = TrimOptions.Trim,
                Quote = '\0',
                BadDataFound = args => { /*Console.Write($"Bad row found: {args.RawRecord}");*/ },
            };

            // using var reader = new StringReader("""
            //     tconst	titleType	primaryTitle	originalTitle	isAdult	startYear	endYear	runtimeMinutes	genres
            //     tt10813978	tvEpisode	Y- the total ease of altering current flow at a given frequency and voltage. The reciprocal of impedance. A quantity, which in rectangular form is as useful for parallel circuitry as independence, is for circuits. The resultant of conductance and subsidence in parallel. The resultant of reciprocal resistance and reciprocal reactance in parallel.	Y- the total ease of altering current flow at a given frequency and voltage. The reciprocal of impedance. A quantity, which in rectangular form is as useful for parallel circuitry as independence, is for circuits. The resultant of conductance and subsidence in parallel. The resultant of reciprocal resistance and reciprocal reactance in parallel.	0	2019	\N	\N	Biography,Comedy,Drama
            //     tt10233364	tvEpisode	"Rolling in the Deep Dish	"Rolling in the Deep Dish	0	2019	\N	\N	Reality-TV
            //     tt10514794	tvEpisode	"Stranger Things" Seasons 1 & 2 in Under 4 Minutes	"Stranger Things" Seasons 1 & 2 in Under 4 Minutes	0	2019	\N	\N	News,Short
            //     tt0073045	movie	"Giliap"	"Giliap"	0	1975	\N	137	Crime,Drama
            //     
            //     """);
            using (var reader = new StreamReader(filePath))
            using (var csv = new CsvReader(reader, configuration))
            {
                csv.Context.RegisterClassMap<TMap>();
                foreach (var item in csv.GetRecords<T>())
                {
                    yield return item;
                }
            }
        }

        private async Task SaveJobsAsync(IEnumerable<Job> jobs)
        {
            string[] categories = ["actor", "actress", "director"];
            foreach (var fewJobs in jobs.Where(j => categories.Contains(j.Category)).Chunk(1000))
            {
                var inserts = fewJobs.Chunk(200).AsParallel().Select(jobsRepository.SaveAsync);
                await Task.WhenAll(inserts);
            }
        }
    }
}

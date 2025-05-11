using CsvHelper.Configuration;

using GraphDatabaseTryout.Data.Converters;

namespace GraphDatabaseTryout.Data.Models
{
    internal record Job(int MovieId, int PersonId, string Category, string Character);

    internal sealed class JobMap : ClassMap<Job>
    {
        public JobMap()
        {
            Parameter(nameof(Job.MovieId)).Name("tconst").TypeConverter(new IdConverter('t'));
            Parameter(nameof(Job.PersonId)).Name("nconst").TypeConverter(new IdConverter('n', 'm'));
            Parameter(nameof(Job.Category)).Name("category");
            Parameter(nameof(Job.Character)).Name("characters").TypeConverter(new TrimConverter('[', ']', '"'));
        }
    }
}

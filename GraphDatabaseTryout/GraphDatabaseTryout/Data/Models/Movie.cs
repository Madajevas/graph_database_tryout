using CsvHelper.Configuration;

namespace GraphDatabaseTryout.Data.Models
{
    internal record Movie(string TConst, string Type, string Name, bool IsAdult, uint? Year, uint? Length, string Genres);
    internal sealed class MovieMap : ClassMap<Movie>
    {
        public MovieMap()
        {
            Parameter(nameof(Movie.TConst)).Name("tconst");
            Parameter(nameof(Movie.Type)).Name("titleType");
            Parameter(nameof(Movie.Name)).Name("originalTitle");
            Parameter(nameof(Movie.IsAdult)).Name("isAdult").TypeConverter<NullableTypeConverter<bool>>();
            Parameter(nameof(Movie.Year)).Name("startYear").TypeConverter<NullableTypeConverter<uint>>();
            Parameter(nameof(Movie.Length)).Name("runtimeMinutes").TypeConverter<NullableTypeConverter<uint>>();
            Parameter(nameof(Movie.Genres)).Name("genres");
        }
    }
}

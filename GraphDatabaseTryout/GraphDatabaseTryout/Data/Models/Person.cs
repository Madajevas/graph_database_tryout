using CsvHelper.Configuration;
using GraphDatabaseTryout.Data.Converters;

namespace GraphDatabaseTryout.Data.Models
{
    internal sealed record Person(int Id, string Name, ushort? BirthYear, ushort? DeathYear);

    internal sealed class PersonMap : ClassMap<Person>
    {
        public PersonMap()
        {
            Parameter(nameof(Person.Id)).Name("nconst").TypeConverter(new IdConverter('n', 'm'));
            Parameter(nameof(Person.Name)).Name("primaryName");
            Parameter(nameof(Person.BirthYear)).Name("birthYear").TypeConverter<NullableTypeConverter<ushort>>();
            Parameter(nameof(Person.DeathYear)).Name("deathYear").TypeConverter<NullableTypeConverter<ushort>>();
        }
    }
}

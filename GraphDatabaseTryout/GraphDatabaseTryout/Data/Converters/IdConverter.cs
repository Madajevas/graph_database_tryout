using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.TypeConversion;

namespace GraphDatabaseTryout.Data.Converters
{
    internal sealed class IdConverter(params char[] trimChars) : ITypeConverter
    {
        public object? ConvertFromString(string? text, IReaderRow row, MemberMapData memberMapData)
        {
            return int.Parse(text.Trim(trimChars));
        }

        public string? ConvertToString(object? value, IWriterRow row, MemberMapData memberMapData)
        {
            throw new NotImplementedException();
        }
    }
}

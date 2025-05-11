using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.TypeConversion;

namespace GraphDatabaseTryout.Data.Converters
{
    internal sealed class TrimConverter(params char[] charsToTrim) : ITypeConverter
    {
        public object? ConvertFromString(string? text, IReaderRow row, MemberMapData memberMapData)
        {
            if (string.IsNullOrEmpty(text) || text == @"\N")
            {
                return null;
            }

            return text?.Trim(charsToTrim);
        }

        public string? ConvertToString(object? value, IWriterRow row, MemberMapData memberMapData)
        {
            throw new NotImplementedException();
        }
    }
}

using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.TypeConversion;

namespace GraphDatabaseTryout.Data.Converters
{
    /// <summary>
    /// Works with strings
    /// </summary>
    internal class ArrayConverter : ITypeConverter
    {
        public object? ConvertFromString(string? text, IReaderRow row, MemberMapData memberMapData)
        {
            if (string.IsNullOrEmpty(text) || text == @"\N")
            {
                return Array.Empty<string>();
            }

            return text.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }

        public string? ConvertToString(object? value, IWriterRow row, MemberMapData memberMapData)
        {
            throw new NotImplementedException();
        }
    }
}

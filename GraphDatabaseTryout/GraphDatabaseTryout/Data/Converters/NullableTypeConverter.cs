using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.TypeConversion;

using System.Globalization;

namespace GraphDatabaseTryout.Data.Converters
{
    internal class NullableTypeConverter<T> : ITypeConverter where T : struct
    {
        public object? ConvertFromString(string? text, IReaderRow row, MemberMapData memberMapData)
        {
            if (string.IsNullOrEmpty(text) || text == @"\N")
            {
                return null;
            }

            if (typeof(T) == typeof(uint))
            {
                return uint.Parse(text, CultureInfo.InvariantCulture);
            }

            if (typeof(T) == typeof(bool))
            {
                return text == "1";
            }

            if (typeof(T) == typeof(ushort))
            {
                return ushort.Parse(text, CultureInfo.InvariantCulture);
            }

            throw new InvalidOperationException("not implemented conversion");
        }

        public string? ConvertToString(object? value, IWriterRow row, MemberMapData memberMapData)
        {
            throw new NotImplementedException();
        }
    }
}

namespace GraphDatabaseTryout.Data
{
    /// <summary>
    /// Quotes breaks reading process. Since they are not important - remove them
    /// </summary>
    internal class RemoveQuotesStream : TextReader
    {
        private readonly TextReader reader;

        public RemoveQuotesStream(TextReader reader)
        {
            this.reader = reader;
        }

        public override int Read(char[] buffer, int index, int count)
        {
            var charsRead = base.Read(buffer, index, count);

            for (var i = 0; i < charsRead; i++)
            {
                if (buffer[i] == '"')
                {
                    buffer[i] = ' ';
                }
            }

            return charsRead;
        }

        public override int Read() => reader.Read();
    }
}

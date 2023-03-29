using Dapper;

using System.Data;
using System.Text;

namespace GraphDatabaseTryout.Data.Repositories
{
    internal class MovieToGenreEdgesRepository
    {
        private const int BatchSize = 20;
        private string insertAssociationSql = GetBulkInsertQuery(BatchSize);

        private readonly IDbConnection connection;

        public MovieToGenreEdgesRepository(IDbConnection connection)
        {
            this.connection = connection;
        }

        public async Task AssociateAsync(IEnumerable<(string movieNodeId, IEnumerable<string> genreNodeIds)> movieToGenreAssociations)
        {
            foreach (var associationsChunk in movieToGenreAssociations.SelectMany(a => a.genreNodeIds.Select(g => (a.movieNodeId, g))).Chunk(BatchSize))
            {
                var insertSql = associationsChunk.Length == BatchSize ? insertAssociationSql : GetBulkInsertQuery(associationsChunk.Length);

                await connection.ExecuteAsync(insertSql, GetParameters(associationsChunk));
            }
        }

        private static string GetBulkInsertQuery(int batchSize)
        {
            var builder = new StringBuilder("INSERT INTO is_of VALUES");
            for (var i = 0; i < batchSize; i++)
            {
                builder.AppendFormat("(@Movie{0}, @Genre{0}),", i);
            }

            builder.Length -= 1;

            return builder.ToString();
        }

        private static DynamicParameters GetParameters(IEnumerable<(string movieNodeId, string genreNodeId)> associations)
        {
            var i = 0;
            var parameter = new DynamicParameters();

            foreach (var association in associations)
            {
                parameter.Add($"@Movie{i}", association.movieNodeId);
                parameter.Add($"@Genre{i}", association.genreNodeId);
                i++;
            }

            return parameter;
        }
    }
}

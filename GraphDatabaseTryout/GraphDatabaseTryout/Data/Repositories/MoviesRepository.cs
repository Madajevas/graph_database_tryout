using Dapper;

using GraphDatabaseTryout.Data.Models;

using System.Data;

namespace GraphDatabaseTryout.Data.Repositories
{
    internal class MoviesRepository
    {
        private const string insertSql = """
            INSERT INTO movie (ID, name, year, length) OUTPUT Inserted.$node_id
            VALUES (@TConst, @Name, @Year, @Length)
            """;

        private readonly IDbConnection connection;

        public MoviesRepository(IDbConnection connection)
        {
            this.connection = connection;

            Dapper.SqlMapper.AddTypeMap(typeof(uint), DbType.Int32);
        }

        // TODO: bulk insert?
        public string Save(Movie movie)
        {
            return connection.ExecuteScalar<string>(insertSql, movie);
        }
    }
}

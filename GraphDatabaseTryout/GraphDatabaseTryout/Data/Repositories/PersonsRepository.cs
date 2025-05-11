using Dapper;

using GraphDatabaseTryout.Data.Models;

using Microsoft.Extensions.ObjectPool;

using System.Data;
using System.Diagnostics.Metrics;

namespace GraphDatabaseTryout.Data.Repositories
{
    internal class PersonsRepository
    {
        private static Meter meter = new Meter("Test.Metrics");
        private static Counter<int> personsCounter = meter.CreateCounter<int>("Persons.Inserted", "person");

        private const string insertSql = $"""
            INSERT INTO person (ID, name, birth_year, death_year) OUTPUT Inserted.$node_id
            VALUES (@{nameof(Person.Id)}, @{nameof(Person.Name)}, @{nameof(Person.BirthYear)}, @{nameof(Person.DeathYear)})
            """;

        private readonly ObjectPool<IDbConnection> connectionPool;

        public PersonsRepository(ObjectPool<IDbConnection> connectionPool)
        {
            this.connectionPool = connectionPool;

            Dapper.SqlMapper.AddTypeMap(typeof(ushort?), DbType.Int16);
            Dapper.SqlMapper.AddTypeMap(typeof(ushort), DbType.Int16);
        }

        public async Task SaveAsync(IReadOnlyCollection<Person> persons)
        {
            var connection = connectionPool.Get();
            using var transaction = connection.BeginTransaction();

            foreach (var person in persons)
            {
                var personNodeId = await connection.ExecuteScalarAsync<string>(insertSql, person, transaction);
            }

            transaction.Commit();
            connectionPool.Return(connection);
            personsCounter.Add(persons.Count);
        }
    }
}

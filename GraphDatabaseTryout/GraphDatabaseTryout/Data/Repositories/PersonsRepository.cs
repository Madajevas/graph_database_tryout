using GraphDatabaseTryout.Data.Models;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;

using System.Data;
using System.Diagnostics.Metrics;
using System.Threading.Tasks.Dataflow;

namespace GraphDatabaseTryout.Data.Repositories
{
    internal class PersonsRepository
    {
        private static Meter meter = new Meter("Test.Metrics");
        private static Counter<int> personsCounter = meter.CreateCounter<int>("Persons.Inserted", "person");

        private readonly IServiceProvider provider;

        public PersonsRepository(IServiceProvider provider)
        {
            this.provider = provider;

            Dapper.SqlMapper.AddTypeMap(typeof(ushort?), DbType.Int16);
            Dapper.SqlMapper.AddTypeMap(typeof(ushort), DbType.Int16);
        }

        public async Task SaveAsync(IEnumerable<Person> persons)
        {
            var blockOptions = new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 16, BoundedCapacity = 5 };
            var linkOptions = new DataflowLinkOptions { PropagateCompletion = true };

            var batchBock = new BatchBlock<Person>(200, new GroupingDataflowBlockOptions { BoundedCapacity = 1000 });

            var insertPersonsBlock = new TransformBlock<IReadOnlyCollection<Person>, int>(async persons =>
            {
                using var connection = provider.GetRequiredService<SqlConnection>();
                await connection.OpenAsync();
                using var transaction = connection.BeginTransaction();

                var sqlBulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction)
                {
                    DestinationTableName = "person",
                    BatchSize = 200,
                };
                sqlBulkCopy.ColumnMappings.Add(nameof(Person.Id), "ID");
                sqlBulkCopy.ColumnMappings.Add(nameof(Person.Name), "name");
                sqlBulkCopy.ColumnMappings.Add(nameof(Person.BirthYear), "birth_year");
                sqlBulkCopy.ColumnMappings.Add(nameof(Person.DeathYear), "death_year");

                var table = new DataTable();
                table.Columns.Add(nameof(Person.Id), typeof(int));
                table.Columns.Add(nameof(Person.Name), typeof(string));
                table.Columns.Add(nameof(Person.BirthYear), typeof(ushort));
                table.Columns.Add(nameof(Person.DeathYear), typeof(ushort));
                foreach (var person in persons)
                {
                    var row = table.NewRow();
                    row[nameof(Person.Id)] = person.Id;
                    row[nameof(Person.Name)] = person.Name;
                    row[nameof(Person.BirthYear)] = person.BirthYear.HasValue ? person.BirthYear.Value : DBNull.Value;
                    row[nameof(Person.DeathYear)] = person.DeathYear.HasValue ? person.DeathYear.Value : DBNull.Value;
                    table.Rows.Add(row);
                }

                await sqlBulkCopy.WriteToServerAsync(table);

                transaction.Commit();

                return persons.Count;
            }, blockOptions);
            batchBock.LinkTo(insertPersonsBlock, linkOptions);
            var reportBlock = new ActionBlock<int>(personsCounter.Add);
            insertPersonsBlock.LinkTo(reportBlock, linkOptions);

            foreach (var person in persons)
            {
                await batchBock.SendAsync(person);
            }
            batchBock.Complete();
            await reportBlock.Completion;
        }
    }
}

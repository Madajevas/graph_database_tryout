using Dapper;

using GraphDatabaseTryout.Data.Models;

using Microsoft.Extensions.ObjectPool;

using System.Data;
using System.Diagnostics.Metrics;

namespace GraphDatabaseTryout.Data.Repositories
{
    internal sealed class JobsRepository(ObjectPool<IDbConnection> connectionPool)
    {
        private static Meter meter = new Meter("Test.Metrics");
        private static Counter<int> personsCounter = meter.CreateCounter<int>("Job.Inserted", "person");

        private const string insertActorSql = """
            INSERT INTO starred_in
            SELECT 
                (SELECT $node_id FROM person WHERE ID = @PersonId),
                (SELECT $node_id FROM movie WHERE ID = @MovieId),
                @Character
            WHERE EXISTS (SELECT 1 FROM person WHERE ID = @PersonId)
              AND EXISTS (SELECT 1 FROM movie WHERE ID = @MovieId)
            """;
        private const string insertDirectorSql = """
            INSERT INTO directed
            SELECT 
                (SELECT $node_id FROM person WHERE ID = @PersonId),
                (SELECT $node_id FROM movie WHERE ID = @MovieId)
            WHERE EXISTS (SELECT 1 FROM person WHERE ID = @PersonId)
              AND EXISTS (SELECT 1 FROM movie WHERE ID = @MovieId)
            """;

        public async Task SaveAsync(IReadOnlyCollection<Job> jobs)
        {
            var connection = connectionPool.Get();
            using var transaction = connection.BeginTransaction();

            foreach (var job in jobs)
            {
                if (job.Category == "actor" ||  job.Category == "actress")
                {
                    await connection.ExecuteAsync(insertActorSql, job, transaction);
                }
                else if (job.Category == "director")
                {
                    await connection.ExecuteAsync(insertDirectorSql, job, transaction);
                }
            }

            transaction.Commit();
            connectionPool.Return(connection);
            personsCounter.Add(jobs.Count);
        }
    }
}

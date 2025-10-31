using GraphDatabaseTryout.Data.Models;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;

using System.Diagnostics.Metrics;
using System.Threading.Tasks.Dataflow;
using System.Transactions;

namespace GraphDatabaseTryout.Data.Repositories
{
    internal sealed class JobsRepository(IServiceProvider provider)
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

        public async Task SaveAsync(IEnumerable<Job> jobs)
        {
            var blockOptions = new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 16, BoundedCapacity = 5 };
            var linkOptions = new DataflowLinkOptions { PropagateCompletion = true };

            IEnumerable<(string sql, Job job)> ToSql(Job job)
            {
                if (job.Category == "actor" || job.Category == "actress")
                {
                    yield return (insertActorSql, job);
                }
                else if (job.Category == "director")
                {
                    yield return (insertDirectorSql, job);
                }
            }
            var toSqlBlock = new TransformManyBlock<Job, (string sql, Job job)>(ToSql, blockOptions);
            var batchBock = new BatchBlock<(string sql, Job job)>(1000, new GroupingDataflowBlockOptions { BoundedCapacity = 1000 });
            toSqlBlock.LinkTo(batchBock, linkOptions);
            var insertJobsBlock = new TransformBlock<IReadOnlyCollection<(string sql, Job job)>, int>(async jobs =>
            {
                using var connection = provider.GetRequiredService<SqlConnection>();
                await connection.OpenAsync();
                using var transaction = connection.BeginTransaction();

                var batch = connection.CreateBatch();
                batch.Transaction = transaction;
                foreach (var (sql, job) in jobs)
                {
                    var command = batch.CreateBatchCommand();
                    command.CommandText = sql;
                    command.CommandType = System.Data.CommandType.Text;
                    var personIdParameter = command.CreateParameter();
                    personIdParameter.ParameterName = "@PersonId";
                    personIdParameter.Value = job.PersonId;
                    command.Parameters.Add(personIdParameter);
                    var movieIdParameter = command.CreateParameter();
                    movieIdParameter.ParameterName = "@MovieId";
                    movieIdParameter.Value = job.MovieId;
                    command.Parameters.Add(movieIdParameter);
                    if (sql == insertActorSql)
                    {
                        var characterParameter = command.CreateParameter();
                        characterParameter.ParameterName = "@Character";
                        characterParameter.Value = job.Character ?? (object)DBNull.Value;
                        command.Parameters.Add(characterParameter);
                    }
                    batch.BatchCommands.Add(command);
                }
                await batch.ExecuteNonQueryAsync();

                await transaction.CommitAsync();

                return jobs.Count;

            }, blockOptions);
            batchBock.LinkTo(insertJobsBlock, linkOptions);
            var reportBlock = new ActionBlock<int>(personsCounter.Add);
            insertJobsBlock.LinkTo(reportBlock, linkOptions);

            foreach (var job in jobs)
            {
                await toSqlBlock.SendAsync(job);
            }
            toSqlBlock.Complete();
            await reportBlock.Completion;
        }
    }
}

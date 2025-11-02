using GraphDatabaseTryout.Data.Models;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;

using System.Diagnostics.Metrics;
using System.Threading.Tasks.Dataflow;

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

            var toCommandBlock = new TransformManyBlock<Job, SqlBatchCommand>(ToCommand, blockOptions);
            var batchBock = new BatchBlock<SqlBatchCommand>(1000, new GroupingDataflowBlockOptions { BoundedCapacity = 5000 });
            toCommandBlock.LinkTo(batchBock, linkOptions);
            var insertJobsBlock = new TransformBlock<IReadOnlyCollection<SqlBatchCommand>, int>(async commands =>
            {
                using var connection = provider.GetRequiredService<SqlConnection>();
                await connection.OpenAsync();
                using var transaction = connection.BeginTransaction();

                using var batch = connection.CreateBatch();
                batch.Transaction = transaction;
                foreach (var command in commands)
                {
                    batch.BatchCommands.Add(command);
                }
                await batch.ExecuteNonQueryAsync();

                await transaction.CommitAsync();

                return commands.Count;

            }, blockOptions);
            batchBock.LinkTo(insertJobsBlock, linkOptions);
            var reportBlock = new ActionBlock<int>(personsCounter.Add);
            insertJobsBlock.LinkTo(reportBlock, linkOptions);

            foreach (var job in jobs)
            {
                await toCommandBlock.SendAsync(job);
            }
            toCommandBlock.Complete();
            await reportBlock.Completion;
        }

        private static IEnumerable<SqlBatchCommand> ToCommand(Job job)
        {
            if (job.Category == "actor" || job.Category == "actress")
            {
                var command = CreateBaseCommand(job);
                command.CommandText = insertActorSql;

                var characterParameter = command.CreateParameter();
                characterParameter.ParameterName = "@Character";
                characterParameter.Value = job.Character ?? (object)DBNull.Value;
                command.Parameters.Add(characterParameter);

                yield return command;
            }
            else if (job.Category == "director")
            {
                var command = CreateBaseCommand(job);
                command.CommandText = insertDirectorSql;
                yield return command;
            }
        }

        private static SqlBatchCommand CreateBaseCommand(Job job)
        {
            var command = new SqlBatchCommand();

            var personIdParameter = command.CreateParameter();
            personIdParameter.ParameterName = "@PersonId";
            personIdParameter.Value = job.PersonId;
            command.Parameters.Add(personIdParameter);

            var movieIdParameter = command.CreateParameter();
            movieIdParameter.ParameterName = "@MovieId";
            movieIdParameter.Value = job.MovieId;
            command.Parameters.Add(movieIdParameter);

            return command;
        }
    }
}

using GraphDatabaseTryout.Data.Repositories;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.ObjectPool;

using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;

namespace GraphDatabaseTryout.Data
{
    internal static class DataLoadingDependencyInjection
    {
        public static void AddDataLoading(this IServiceCollection services)
        {
            services.AddTransient<IDbConnection>(_ =>
            {
                var connectionString = Environment.GetEnvironmentVariable("ConnectionString:Graph")
                    ?? throw new ArgumentException("Connection string missing");

                return new SqlConnection(connectionString);
            });
            services.Decorate<IDbConnection, DbConnectionProxy>();


            services.TryAddSingleton<ObjectPool<IDbConnection>>(serviceProvider =>
            {
                var provider = serviceProvider.GetRequiredService<ObjectPoolProvider>();

                return provider.Create(new DbConnectionsPoolPolicy(serviceProvider));
            });


            services.AddScoped<GenresRepository>();
            services.AddScoped<MoviesRepository>();
            services.AddScoped<MovieToGenreEdgesRepository>();

            services.AddScoped<DataLoader>();
        }
    }

    internal class DbConnectionsPoolPolicy : IPooledObjectPolicy<IDbConnection>
    {
        private readonly IServiceProvider serviceProvider;

        private readonly ConcurrentStack<IDbConnection> connections = new ConcurrentStack<IDbConnection>();

        public DbConnectionsPoolPolicy(IServiceProvider serviceProvider)
        {
            this.serviceProvider = serviceProvider;
        }

        public IDbConnection Create()
        {
            if (connections.TryPop(out var connection))
            {
                return connection;
            }

            return serviceProvider.GetService<IDbConnection>()!;
        }

        public bool Return(IDbConnection obj)
        {
            connections.Push(obj);
            return true;
        }
    }

    internal class DbConnectionProxy : DbConnection, IDisposable
    {
        private readonly IDbConnection connection;

        public DbConnectionProxy(IDbConnection connection)
        {
            this.connection = connection;

            Open();  // dapper opens and closes connection, concurrently it causes troubles
        }

        public void Dispose()
        {
            Close();
        }

        public override string ConnectionString { get => connection.ConnectionString; set => connection.ConnectionString = value; }

        public override string Database => connection.Database;

        public override string DataSource => (connection as DbConnection).DataSource;

        public override string ServerVersion => (connection as DbConnection).ServerVersion;

        public override ConnectionState State => connection.State;

        public override void ChangeDatabase(string databaseName)
        {
            connection.ChangeDatabase(databaseName);
        }

        public override void Close()
        {
            connection?.Close();
        }

        public override void Open()
        {
            connection.Open();
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            return connection.BeginTransaction() as DbTransaction;
        }

        protected override DbCommand CreateDbCommand()
        {
            return connection.CreateCommand() as DbCommand;
        }
    }
}

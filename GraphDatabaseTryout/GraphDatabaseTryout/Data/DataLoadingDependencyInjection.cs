using GraphDatabaseTryout.Data.Repositories;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.ObjectPool;

using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Diagnostics;

namespace GraphDatabaseTryout.Data
{
    internal static class DataLoadingDependencyInjection
    {
        public static void AddDataLoading(this IServiceCollection services, string connectionString)
        {
            services.AddTransient<SqlConnection>(_ => new SqlConnection(connectionString));

            services.TryAddSingleton<ObjectPool<IDbConnection>>(serviceProvider =>
            {
                return new DefaultObjectPool<IDbConnection>(new DbConnectionsPoolPolicy(serviceProvider), 100);
                var provider = serviceProvider.GetRequiredService<ObjectPoolProvider>();
                return provider.Create(new DbConnectionsPoolPolicy(serviceProvider));
            });


            services.AddScoped<GenresRepository>();
            services.AddScoped<MoviesRepository>();
            services.AddScoped<PersonsRepository>();
            services.AddScoped<JobsRepository>();

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
            Debug.Assert(obj.State == ConnectionState.Open, "Connection is not open");
            connections.Push(obj);
            return true;
        }
    }
}

using GraphDatabaseTryout.Data.Repositories;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;

using System.Data;

namespace GraphDatabaseTryout.Data
{
    internal static class DataLoadingDependencyInjection
    {
        public static void AddDataLoading(this IServiceCollection services)
        {
            services.AddScoped<IDbConnection>(_ =>
            {
                var connectionString = Environment.GetEnvironmentVariable("ConnectionString:Graph")
                    ?? throw new ArgumentException("Connection string missing");

                return new SqlConnection(connectionString);
            });

            services.AddScoped<GenresRepository>();
            services.AddScoped<MoviesRepository>();
            services.AddScoped<MovieToGenreEdgesRepository>();

            services.AddScoped<DataLoader>();
        }
    }
}

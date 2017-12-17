using System;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using System.Data.SqlClient;
using Npgsql;

namespace dexih.operations.tests
{
    public sealed class TestContextFactory : IDisposable
    {
        private IDisposable _connection;
        private IDisposable _scope;

        public TContext CreateInMemoryDatabase<TContext>() where TContext : DbContext
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection
                .AddDbContext<TContext>(c => c.UseInMemoryDatabase(GetUniqueName()));
            var serviceProvider = serviceCollection.BuildServiceProvider();
            var scope = serviceProvider.GetRequiredService<IServiceScopeFactory>().CreateScope();
            _scope = scope;
            return scope.ServiceProvider.GetService<TContext>();
        }

        public TContext CreateInMemorySqlite<TContext>(bool migrate = true) where TContext : DbContext
        {
            var connectionString = $"Data Source={GetUniqueName()};Mode=Memory;Cache=Shared";
            var connection = OpenConnectionToKeepInMemoryDbUntilDispose(connectionString);
            var dbContext = CreateSqliteDbContext<TContext>(connection);
            if (migrate)
            {
                dbContext.Database.EnsureCreated();
                dbContext.Database.Migrate();
            }
            return dbContext;
        }

		public TContext CreateProgreSql<TContext>(bool migrate = true) where TContext : DbContext
		{
			var connectionString = $"host=localhost;port=5432;Username=dexih;Password=landmark;pooling=true";
			var database = $"testdb_{Guid.NewGuid().ToString()}";

			//create a test database.
			using (var sqlCon = new NpgsqlConnection(connectionString))
			{
				sqlCon.Open();

				using (var sqlCommand = new NpgsqlCommand($"create database \"{database}\"", sqlCon))
				{
					sqlCommand.ExecuteNonQuery();
				}
			}

			connectionString = connectionString + $";Database={database};";
			DbConnection connection = new NpgsqlConnection(connectionString);
			connection.Open();
			_connection = connection;

			var dbContext = CreatePostgreSqlDbContext<TContext>(connection);
			if (migrate)
			{
				dbContext.Database.EnsureCreated();
			}
			return dbContext;
		}

        public TContext CreateSqlServer<TContext>(bool migrate = true) where TContext : DbContext
        {
            var connectionString = $"Data Source=.; Integrated Security=True;";
            var database = $"testdb_{Guid.NewGuid().ToString()}";

            //create a test database.
            using (var sqlCon = new SqlConnection(connectionString))
            {
                sqlCon.Open();
                
                using (var sqlCommand = new SqlCommand($"create database [{database}]", sqlCon))
                {
                    sqlCommand.ExecuteNonQuery();
                }
            }

            connectionString = $"Data Source=.;Integrated Security=True;Initial Catalog={database};";
            DbConnection connection = new SqlConnection(connectionString);
            connection.Open();
            _connection = connection;

            var dbContext = CreateSqlServerDbContext<TContext>(connection);
            if (migrate)
            {
                dbContext.Database.EnsureCreated();
            }
            return dbContext;
        }


        private string GetUniqueName()
        {
            return $"testdb_{Guid.NewGuid().ToString()}.db";
        }

        private DbConnection OpenConnectionToKeepInMemoryDbUntilDispose(string connectionString)
        {
            var connection = new SqliteConnection(connectionString);
            connection.Open();
            _connection = connection;
            return connection;
        }

        private TContext CreateSqliteDbContext<TContext>(DbConnection connection) where TContext : DbContext
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection
                .AddDbContext<TContext>(c => c.UseSqlite(connection));
            IServiceProvider serviceProvider = serviceCollection.BuildServiceProvider();
            var scope = serviceProvider.GetRequiredService<IServiceScopeFactory>().CreateScope();
            _scope = scope;
            return scope.ServiceProvider.GetService<TContext>();
        }

        private TContext CreateSqlServerDbContext<TContext>(DbConnection connection) where TContext : DbContext
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection
                .AddDbContext<TContext>(c => c.UseSqlServer(connection));
            IServiceProvider serviceProvider = serviceCollection.BuildServiceProvider();
            var scope = serviceProvider.GetRequiredService<IServiceScopeFactory>().CreateScope();
            _scope = scope;
            return scope.ServiceProvider.GetService<TContext>();
        }

		private TContext CreatePostgreSqlDbContext<TContext>(DbConnection connection) where TContext : DbContext
		{
			var serviceCollection = new ServiceCollection();
			serviceCollection
				.AddDbContext<TContext>(c => c.UseNpgsql(connection));
			IServiceProvider serviceProvider = serviceCollection.BuildServiceProvider();
			var scope = serviceProvider.GetRequiredService<IServiceScopeFactory>().CreateScope();
			_scope = scope;
			return scope.ServiceProvider.GetService<TContext>();
		}

        public void Dispose()
        {
            _connection?.Dispose();
            _scope?.Dispose();
        }
    }
}
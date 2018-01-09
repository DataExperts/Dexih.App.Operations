using dexih.functions;
using dexih.repository;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using static dexih.repository.DexihDatalinkTable;

namespace dexih.operations
{
    public class CacheManager
    {
        public DexihHub DexihHub { get; set; }
        public ICollection<DexihDatabaseType> DexihDatabaseTypes { get; set; }
        public ICollection<DexihTransform> DexihTransforms { get; set; }
        public ICollection<DexihProfileRule> DexihProfileRules { get; set; }
        public ICollection<DexihStandardFunction> DexihStandardFunctions { get; set; }
        public ICollection<DexihUpdateStrategy> DexihUpdateStrategies { get; set; }

        public string BuildVersion { get; set; }
        public DateTime BuildDate { get; set; }

		public CacheManager()
		{
			
		}

        public CacheManager(long hubKey, string cacheEncryptionKey)
        {
            HubKey = hubKey;
            CacheEncryptionKey = cacheEncryptionKey;
            Initialize(hubKey);
        }

        public CacheManager(long hubKey, string cacheEncryptionKey, string secondaryEncryptionKey)
        {
            CacheEncryptionKey = cacheEncryptionKey;
            SecondaryEncryptionKey = secondaryEncryptionKey;
            Initialize(hubKey);
        }

        private void Initialize(long hubKey)
        {
            DexihHub = new DexihHub() { HubKey = hubKey };
            DexihDatabaseTypes = new HashSet<DexihDatabaseType>();
            DexihTransforms = new HashSet<DexihTransform>();
            DexihProfileRules = new HashSet<DexihProfileRule>();
            DexihStandardFunctions = new HashSet<DexihStandardFunction>();
		}


        public long HubKey { get; private set; }
        /// <summary>
        /// Key used to encrypt cache fields (such as passwords) when they are saved to repository or json file.
        /// </summary>
        public string CacheEncryptionKey { get; protected set; }
        public string SecondaryEncryptionKey { get; protected set; }

        public async Task<DexihHub> InitHub(DexihRepositoryContext dbContext)
        {
            DexihHub = await dbContext.DexihHubs.SingleOrDefaultAsync(c => c.HubKey == HubKey && c.IsValid);
            DexihHub.DexihHubVariables.Clear();
            DexihHub.DexihConnections.Clear();
            DexihHub.DexihDatajobs.Clear();
            DexihHub.DexihFileFormats.Clear();
            DexihHub.DexihHubUsers.Clear();
            DexihHub.DexihDatalinks.Clear();

            return DexihHub;
        }
        
     public async Task<DexihHub> LoadHubSharedObjects(DexihRepositoryContext dbContext)
        {
			try
			{

                await InitHub(dbContext);

				if (DexihHub == null)
				{
                    throw new CacheManagerException($"The hub with the key {HubKey} could not be found in the repository.");
				}

                var variables = dbContext.DexihHubVariable
                    .Where(c => c.IsValid && c.HubKey == HubKey);

                DexihHub.DexihHubVariables = await variables.ToArrayAsync();

                // load connections
                var connections = dbContext.DexihConnections
					.Include(c => c.DatabaseType)
					.Where(c => c.IsValid && c.HubKey == HubKey && !c.IsInternal);

				await dbContext.DexihTables
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

				await dbContext.DexihTableColumns
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

				DexihHub.DexihConnections = await connections.ToArrayAsync();


                // load the datalinks
                var datalinks = dbContext.DexihDatalinks
                    .Include(c => c.UpdateStrategy)
                    .Where(c => c.IsValid && c.HubKey == HubKey);

                await dbContext.DexihDatalinkTables
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

                await dbContext.DexihDatalinkColumns
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

				await dbContext.DexihDatalinkTransforms
					.Include(c => c.Transform)
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

				await dbContext.DexihDatalinkTransformItems
                    .Include(c => c.StandardFunction)
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .OrderBy(c => c.Dt.Datalink.HubKey).ThenBy(c => c.Dt.DatalinkTransformKey).ThenBy(c => c.DatalinkTransformItemKey)
					.LoadAsync();

				await dbContext.DexihFunctionParameters
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .OrderBy(c => c.DtItem.Dt.Datalink.HubKey).ThenBy(c => c.DtItem.Dt.DatalinkTransformKey).ThenBy(c => c.DtItem.DatalinkTransformItemKey).ThenBy(c => c.Position)
					.LoadAsync();

				await dbContext.DexihDatalinkProfiles
					.Include(c => c.ProfileRule)
					.Where(c => c.IsValid && c.Datalink.IsValid && c.Datalink.HubKey == HubKey)
					.LoadAsync();

                DexihHub.DexihDatalinks = await datalinks.ToArrayAsync();

				// load the datajobs with all dependent schedules/datalink steps.
				var datajobs = dbContext.DexihDatajobs
					.Where(c => c.IsValid && c.HubKey == HubKey);

				await dbContext.DexihDatalinkStep
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

				await dbContext.DexihDatalinkDependencies
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

				await dbContext.DexihTriggers
					.Where(c => c.IsValid && c.Datajob.IsValid && c.Datajob.HubKey == HubKey)
					.LoadAsync();

				DexihHub.DexihDatajobs = await datajobs.ToArrayAsync();

				var internalHub = await dbContext.DexihHubs.FirstAsync(c => c.IsValid && c.IsInternal);
				DexihHub.DexihFileFormats = await dbContext.DexihFileFormat.Where(c => (c.HubKey == HubKey || c.HubKey == internalHub.HubKey) && c.IsValid).ToArrayAsync();
				DexihHub.DexihColumnValidations = await dbContext.DexihColumnValidation.Where(c => c.HubKey == HubKey && c.IsValid).ToArrayAsync();

				return DexihHub;
			} catch(Exception ex)
			{
                throw new CacheManagerException($"An error occurred trying to load the hub.  {ex.Message}", ex);
			}
        }

        public async Task<DexihHub> LoadHub(DexihRepositoryContext dbContext)
        {
			try
			{

                await InitHub(dbContext);

				if (DexihHub == null)
				{
                    throw new CacheManagerException($"The hub with the key {HubKey} could not be found in the repository.");
				}

                var variables = dbContext.DexihHubVariable
                    .Where(c => c.IsValid && c.HubKey == HubKey);

                DexihHub.DexihHubVariables = await variables.ToArrayAsync();

                // load connections
                var connections = dbContext.DexihConnections
					.Include(c => c.DatabaseType)
					.Where(c => c.IsValid && c.HubKey == HubKey && !c.IsInternal);

				await dbContext.DexihTables
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

				await dbContext.DexihTableColumns
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

				DexihHub.DexihConnections = await connections.ToArrayAsync();


                // load the datalinks
                var datalinks = dbContext.DexihDatalinks
                    .Include(c => c.UpdateStrategy)
                    .Where(c => c.IsValid && c.HubKey == HubKey);

                await dbContext.DexihDatalinkTables
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

                await dbContext.DexihDatalinkColumns
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

				await dbContext.DexihDatalinkTransforms
					.Include(c => c.Transform)
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

				await dbContext.DexihDatalinkTransformItems
                    .Include(c => c.StandardFunction)
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .OrderBy(c => c.Dt.Datalink.HubKey).ThenBy(c => c.Dt.DatalinkTransformKey).ThenBy(c => c.DatalinkTransformItemKey)
					.LoadAsync();

				await dbContext.DexihFunctionParameters
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .OrderBy(c => c.DtItem.Dt.Datalink.HubKey).ThenBy(c => c.DtItem.Dt.DatalinkTransformKey).ThenBy(c => c.DtItem.DatalinkTransformItemKey).ThenBy(c => c.Position)
					.LoadAsync();

				await dbContext.DexihDatalinkProfiles
					.Include(c => c.ProfileRule)
					.Where(c => c.IsValid && c.Datalink.IsValid && c.Datalink.HubKey == HubKey)
					.LoadAsync();

                DexihHub.DexihDatalinks = await datalinks.ToArrayAsync();

				// load the datajobs with all dependent schedules/datalink steps.
				var datajobs = dbContext.DexihDatajobs
					.Where(c => c.IsValid && c.HubKey == HubKey);

				await dbContext.DexihDatalinkStep
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

				await dbContext.DexihDatalinkDependencies
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

				await dbContext.DexihTriggers
					.Where(c => c.IsValid && c.Datajob.IsValid && c.Datajob.HubKey == HubKey)
					.LoadAsync();

				DexihHub.DexihDatajobs = await datajobs.ToArrayAsync();

				var internalHub = await dbContext.DexihHubs.FirstAsync(c => c.IsValid && c.IsInternal);
				DexihHub.DexihFileFormats = await dbContext.DexihFileFormat.Where(c => (c.HubKey == HubKey || c.HubKey == internalHub.HubKey) && c.IsValid).ToArrayAsync();
				DexihHub.DexihColumnValidations = await dbContext.DexihColumnValidation.Where(c => c.HubKey == HubKey && c.IsValid).ToArrayAsync();

				return DexihHub;
			} catch(Exception ex)
			{
                throw new CacheManagerException($"An error occurred trying to load the hub.  {ex.Message}", ex);
			}
        }

        public async Task<bool> LoadGlobal(DexihRepositoryContext dbContext)
        {
            DexihDatabaseTypes = await dbContext.DexihDatabaseTypes.Where(c => c.IsValid).ToListAsync();
            DexihProfileRules = await dbContext.DexihProfileRules.Where(c => c.IsValid).ToListAsync();
            DexihStandardFunctions = await dbContext.DexihStandardFunctions.Where(c => c.IsValid).ToListAsync();
            DexihTransforms = await dbContext.DexihTransforms.Where(c => c.IsValid).ToListAsync();
            DexihUpdateStrategies = await dbContext.DexihUpdateStrategies.Where(c => c.IsValid).ToListAsync();

            BuildVersion = Assembly.GetEntryAssembly().GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion;
            BuildDate = System.IO.File.GetLastWriteTime(Assembly.GetEntryAssembly().Location);

            return true;
        }

        public async Task LoadConnectionTables(DexihConnection connection, DexihRepositoryContext dbContext)
        {
            await dbContext.Entry(connection).Collection(a => a.DexihTables).Query().Where(c => c.IsValid && connection.ConnectionKey == c.ConnectionKey).LoadAsync();

            foreach (var table in connection.DexihTables.Where(c => c.HubKey == connection.HubKey))
            {
                await LoadTableColumns(table, dbContext);

                if(DexihDatabaseTypes.Count(d=>d.DatabaseTypeKey == connection.DatabaseTypeKey) == 0)
                {
                    DexihDatabaseTypes.Add(await dbContext.DexihDatabaseTypes.SingleAsync(c => c.DatabaseTypeKey == connection.DatabaseTypeKey));
                }
            }
        }

        public async Task LoadTableColumns(DexihTable table, DexihRepositoryContext dbContext)
        {
            await dbContext.Entry(table).Collection(a => a.DexihTableColumns).Query().Where(c => c.IsValid && table.TableKey == c.TableKey).LoadAsync();

        }

        public async Task LoadDatalinkDependencies(DexihDatalink datalink, bool includeDependencies, DexihRepositoryContext dbContext)
        {
            if (includeDependencies)
            {
				if (datalink.SourceDatalinkTable.SourceType == ESourceType.Table && datalink.SourceDatalinkTable.SourceTableKey != null)
				{
					await AddTables(new[] { datalink.SourceDatalinkTable.SourceTableKey.Value }, dbContext);
				}
				else if ( datalink.SourceDatalinkTable.SourceDatalinkKey != null)
				{
					await AddDatalinks(new[] { datalink.SourceDatalinkTable.SourceDatalinkKey.Value }, dbContext);
				}

                if (datalink.TargetTableKey != null)
                {
                    await AddTables(new[] {(long)datalink.TargetTableKey}, dbContext);
                }
                if (datalink.DatalinkType != DexihDatalink.EDatalinkType.Publish)
                {
                    await AddConnections(new[] { datalink.AuditConnectionKey }, false, dbContext);
                }
            }

            await dbContext.Entry(datalink).Collection(a => a.DexihDatalinkTransforms).Query().Where(a => a.IsValid && datalink.DatalinkKey == a.DatalinkKey).Include(c=>c.Transform).Include(c=>c.JoinDatalinkTable).OrderBy(c=>c.Position).LoadAsync();

            foreach (var datalinkTransform in datalink.DexihDatalinkTransforms)
            {
                if (includeDependencies && datalinkTransform.JoinDatalinkTable != null)
                {
                    await dbContext.Entry(datalinkTransform.JoinDatalinkTable).Collection(a => a.DexihDatalinkColumns).Query().Where(a => a.IsValid && datalinkTransform.JoinDatalinkTable.DatalinkTableKey == a.DatalinkTableKey).OrderBy(c=>c.Position).LoadAsync();
                    
					if (datalinkTransform.JoinDatalinkTable.SourceType == ESourceType.Table && datalinkTransform.JoinDatalinkTable.SourceTableKey != null)
					{
						await AddTables(new[] { datalinkTransform.JoinDatalinkTable.SourceTableKey.Value }, dbContext);
					}
					else if  (datalinkTransform.JoinDatalinkTable.SourceType == ESourceType.Datalink && datalinkTransform.JoinDatalinkTable.SourceDatalinkKey != null)
					{
						await AddDatalinks(new[] { datalinkTransform.JoinDatalinkTable.SourceDatalinkKey.Value }, dbContext);
					}
                }

                await dbContext.Entry(datalinkTransform).Collection(a => a.DexihDatalinkTransformItems).Query().Where(a => a.IsValid && datalinkTransform.DatalinkTransformKey == a.DatalinkTransformKey).Include(c => c.TargetDatalinkColumn).OrderBy(c=>c.Position).LoadAsync();
                foreach (var item in datalinkTransform.DexihDatalinkTransformItems)
                {
                    await dbContext.Entry(item).Collection(a => a.DexihFunctionParameters).Query().Where(a => a.IsValid && item.DatalinkTransformItemKey == a.DatalinkTransformItemKey).Include(c=>c.DatalinkColumn).OrderBy(c=>c.Position).LoadAsync();

                    if (includeDependencies)
                    {
                        if (item.StandardFunctionKey != null && DexihStandardFunctions.Count(c => c.StandardFunctionKey == item.StandardFunctionKey) == 0)
                        {
                            var standardFunction = await dbContext.DexihStandardFunctions.SingleOrDefaultAsync(c => c.StandardFunctionKey == item.StandardFunctionKey && c.IsValid);
                            DexihStandardFunctions.Add(standardFunction);
                        }
                    }
                }
            }

            if (includeDependencies)
            {
                foreach (var datalinkProfile in datalink.DexihDatalinkProfiles)
                {
                    if (DexihProfileRules.Count(c => c.ProfileRuleKey == datalinkProfile.ProfileRuleKey) == 0)
                    {
                        var profileRule = await dbContext.DexihProfileRules.SingleOrDefaultAsync(c => c.ProfileRuleKey == datalinkProfile.ProfileRuleKey && c.IsValid);
                        DexihProfileRules.Add(profileRule);
                    }
                }
            }
        }

        private async Task LoadDatajobDependencies(DexihDatajob datajob, bool includeDependencies, DexihRepositoryContext dbContext)
        {
            await dbContext.Entry(datajob).Collection(a => a.DexihTriggers).Query().Where(a => a.IsValid && datajob.DatajobKey == a.DatajobKey).LoadAsync();
            await dbContext.Entry(datajob).Collection(a => a.DexihDatalinkSteps).Query().Where(a => a.IsValid && datajob.DatajobKey == a.DatajobKey).LoadAsync();

            if(includeDependencies)
            {
                await AddConnections(new[] { datajob.AuditConnectionKey }, false, dbContext);

                foreach(var datalinkStep in datajob.DexihDatalinkSteps)
                {
                    await AddDatalinks(datajob.DexihDatalinkSteps.Select(c => c.DatalinkKey).ToArray(), dbContext);
                }
            }
        }

        private async Task LoadColumnValidationDependencies(DexihColumnValidation columnValidation, DexihRepositoryContext dbContext)
        {
            if(columnValidation.LookupColumnKey != null)
            {
                var column = await dbContext.DexihTableColumns.SingleOrDefaultAsync(c => c.ColumnKey == columnValidation.LookupColumnKey);
                await AddTables(new[] { column.TableKey }, dbContext);
            }
        }

        public async Task AddConnections(IEnumerable<long> connectionKeys, bool includeTables, DexihRepositoryContext dbContext)
        {
            foreach (var connectionKey in connectionKeys)
            {
                var connection = DexihHub.DexihConnections.SingleOrDefault(c => c.ConnectionKey == connectionKey);
                if(connection == null)
                {
                    connection = await dbContext.DexihConnections.Include(c=>c.DatabaseType).SingleOrDefaultAsync(c => c.HubKey == HubKey && c.ConnectionKey == connectionKey && c.IsValid);
                    DexihHub.DexihConnections.Add(connection);
                }

                if(includeTables)
                {
                    await LoadConnectionTables(connection, dbContext);
                }
            }
        }

        public async Task AddDatalinks(IEnumerable<long> datalinkKeys, DexihRepositoryContext dbContext)
        {
            var repoManager = new RepositoryManager(CacheEncryptionKey, dbContext);
            var datalinks = await repoManager.GetDatalinks(HubKey, datalinkKeys);
            
            foreach (var datalink in datalinks)
            {
                await LoadDatalinkDependencies(datalink, true, dbContext);
                DexihHub.DexihDatalinks.Add(datalink);
            }
        }

        public async Task AddTables(IEnumerable<long> tableKeys, DexihRepositoryContext dbContext)
        {
            foreach (var tableKey in tableKeys)
            {
                DexihTable table = null;
                foreach(var connection in DexihHub.DexihConnections)
                {
                    table = connection.DexihTables.SingleOrDefault(c => c.TableKey == tableKey);
                    if (table != null) break;
                }

                if(table == null)
                {
                    table = await dbContext.DexihTables.SingleOrDefaultAsync(c => c.HubKey == HubKey && c.TableKey == tableKey && c.IsValid);
                    await LoadTableColumns(table, dbContext);

					await AddConnections(new long[] { table.ConnectionKey }, false, dbContext);
					var connection = DexihHub.DexihConnections.SingleOrDefault(c => c.ConnectionKey == table.ConnectionKey);

					connection.DexihTables.Add(table);
                }

                if(table.FileFormatKey != null)
                {
                    var fileFormat = DexihHub.DexihFileFormats.SingleOrDefault(c => c.FileFormatKey == table.FileFormatKey);
                    if(fileFormat == null)
                    {
                        var internalHub = await dbContext.DexihHubs.FirstAsync(c => c.IsValid && c.IsInternal);
                        fileFormat = await dbContext.DexihFileFormat.SingleOrDefaultAsync(c => (c.HubKey == HubKey || c.HubKey == internalHub.HubKey) && c.FileFormatKey == table.FileFormatKey && c.IsValid);
                        DexihHub.DexihFileFormats.Add(fileFormat);
                    }
                }
                await LoadTableColumns(table, dbContext);
            }
        }

        public async Task AddDatajobs(IEnumerable<long> datajobKeys, DexihRepositoryContext dbContext)
        {
            foreach (var datajobKey in datajobKeys)
            {
                var job = DexihHub.DexihDatajobs.SingleOrDefault(c=>c.DatajobKey == datajobKey);
                if(job == null)
                {
                    job = await dbContext.DexihDatajobs.SingleOrDefaultAsync(c => c.HubKey == HubKey && c.DatajobKey == datajobKey && c.IsValid);
                    DexihHub.DexihDatajobs.Add(job);
                }

                await LoadDatajobDependencies(job, true, dbContext);
            }
        }

        public async Task AddColumnValidations(IEnumerable<long> columnValidationKeys, DexihRepositoryContext dbContext)
        {
            foreach (var columnValidationKey in columnValidationKeys)
            {
                var columnValidation = DexihHub.DexihColumnValidations.SingleOrDefault(c => c.ColumnValidationKey == columnValidationKey);
                if(columnValidation == null)
                {
                    columnValidation = await dbContext.DexihColumnValidation.SingleOrDefaultAsync(c => c.HubKey == HubKey && c.ColumnValidationKey == columnValidationKey && c.IsValid);
                    DexihHub.DexihColumnValidations.Add(columnValidation);
                }

                await LoadColumnValidationDependencies(columnValidation, dbContext);

            }
        }
    }
}

﻿using dexih.functions;
using dexih.repository;
using dexih.transforms;
using dexih.transforms.Transforms;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Dexih.Utils.CopyProperties;

namespace dexih.operations
{
    public class CacheManager
    {
        public DexihHub Hub { get; set; }

        public string BuildVersion { get; set; }
        public DateTime BuildDate { get; set; }

        public string GoogleClientId { get; set; }
        public string MicrosoftClientId { get; set; }
        public string GoogleMapsAPIKey { get; set; }

        public RemoteLibraries DefaultRemoteLibraries { get; set; }

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
            Initialize(hubKey);
        }

        private void Initialize(long hubKey)
        {
            Hub = new DexihHub() { HubKey = hubKey };
		}

        public long HubKey { get; set; }
        
	    /// <summary>
        /// Key used to encrypt cache fields (such as passwords) when they are saved to repository or json file.
        /// </summary>
        public string CacheEncryptionKey { get; set; }

        public async Task<DexihHub> InitHub(DexihRepositoryContext dbContext)
        {
            Hub = await dbContext.DexihHubs.SingleOrDefaultAsync(c => c.HubKey == HubKey && c.IsValid);

	        if (Hub == null)
	        {
		        throw new CacheManagerException($"The hub with the key {HubKey} no longer exists.");
	        }
	        
//            Hub.DexihHubVariables.Clear();
//            Hub.DexihColumnValidations.Clear();
//            Hub.DexihConnections.Clear();
//            Hub.DexihDatajobs.Clear();
//            Hub.DexihFileFormats.Clear();
//            Hub.DexihHubUsers.Clear();
//            Hub.DexihDatalinks.Clear();
//            Hub.DexihCustomFunctions.Clear();

            return Hub;
        }
        
		
	    public async Task<DexihHub> LoadHub(DexihRepositoryContext dbContext)
        {
			try
			{

                await InitHub(dbContext);

				if (Hub == null)
				{
                    throw new CacheManagerException($"The hub with the key {HubKey} could not be found in the repository.");
				}

                var variables = dbContext.DexihHubVariable
                    .Where(c => c.IsValid && c.HubKey == HubKey);

                Hub.DexihHubVariables = await variables.ToArrayAsync();

                // load connections
                var connections = dbContext.DexihConnections
					.Where(c => c.IsValid && c.HubKey == HubKey);

				await dbContext.DexihTables
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

				await dbContext.DexihTableColumns
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

				Hub.DexihConnections = await connections.ToArrayAsync();


                // load the datalinks
                var datalinks = dbContext.DexihDatalinks
                    .Where(c => c.IsValid && c.HubKey == HubKey);

                await dbContext.DexihDatalinkTargets
	                .Where(c => c.IsValid && c.HubKey == HubKey)
	                .LoadAsync();

                await dbContext.DexihDatalinkTables
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

                await dbContext.DexihDatalinkColumns
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

				await dbContext.DexihDatalinkTransforms
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

				await dbContext.DexihDatalinkTransformItems
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .OrderBy(c => c.Dt.Datalink.HubKey).ThenBy(c => c.Dt.DatalinkTransformKey).ThenBy(c => c.DatalinkTransformItemKey)
					.LoadAsync();

				await dbContext.DexihFunctionParameters
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .OrderBy(c => c.DtItem.Dt.Datalink.HubKey).ThenBy(c => c.DtItem.Dt.DatalinkTransformKey).ThenBy(c => c.DtItem.DatalinkTransformItemKey).ThenBy(c => c.Position)
					.LoadAsync();

				await dbContext.DexihFunctionArrayParameters
					.Where(c => c.IsValid && c.HubKey == HubKey)
					.OrderBy(c => c.FunctionParameter.DtItem.Dt.Datalink.HubKey).ThenBy(c => c.FunctionParameter.DtItem.Dt.DatalinkTransformKey).ThenBy(c => c.FunctionParameter.DtItem.DatalinkTransformItemKey).ThenBy(c => c.Position)
					.LoadAsync();

				await dbContext.DexihDatalinkProfiles
					.Where(c => c.IsValid && c.Datalink.IsValid && c.Datalink.HubKey == HubKey)
					.LoadAsync();

                Hub.DexihDatalinks = await datalinks.ToArrayAsync();

				// load the datajobs with all dependent schedules/datalink steps.
				var datajobs = dbContext.DexihDatajobs
					.Where(c => c.IsValid && c.HubKey == HubKey);

				await dbContext.DexihDatalinkStep
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();
				
				await dbContext.DexihDatalinkStepColumns
					.Where(c => c.IsValid && c.HubKey == HubKey)
					.LoadAsync();

				await dbContext.DexihDatalinkDependencies
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

				await dbContext.DexihTriggers
					.Where(c => c.IsValid && c.Datajob.IsValid && c.Datajob.HubKey == HubKey)
					.LoadAsync();

				Hub.DexihDatalinkTests = await dbContext.DexihDatalinkTests
					.Where(c => c.IsValid && c.HubKey == HubKey)
					.ToArrayAsync();

				await dbContext.DexihDatalinkTestSteps
					.Where(c => c.IsValid && c.HubKey == HubKey)
					.LoadAsync();

				await dbContext.DexihDatalinkTestTables
					.Where(c => c.IsValid && c.HubKey == HubKey)
					.LoadAsync();


				Hub.DexihDatajobs = await datajobs.ToArrayAsync();

				Hub.DexihFileFormats = await dbContext.DexihFileFormat.Where(c => (c.HubKey == HubKey) && c.IsValid).ToArrayAsync();
				Hub.DexihColumnValidations = await dbContext.DexihColumnValidation.Where(c => c.HubKey == HubKey && c.IsValid).ToArrayAsync();
			    Hub.DexihCustomFunctions = await dbContext.DexihCustomFunctions.Include(c=>c.DexihCustomFunctionParameters).Where(c => c.HubKey == HubKey && c.IsValid).ToArrayAsync();
			    Hub.DexihRemoteAgentHubs = await dbContext.DexihRemoteAgentHubs.Where(c => c.HubKey == HubKey && c.IsValid).ToArrayAsync();
				Hub.DexihViews = await dbContext.DexihViews.Where(c => c.HubKey == HubKey && c.IsValid).ToArrayAsync();

				return Hub;
			} catch(Exception ex)
			{
                throw new CacheManagerException($"An error occurred trying to load the hub.  {ex.Message}", ex);
			}
        }

        public bool LoadGlobal(DexihRepositoryContext dbContext)
        {
            BuildVersion = Assembly.GetEntryAssembly().GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion;
            BuildDate = System.IO.File.GetLastWriteTime(Assembly.GetEntryAssembly().Location);

            // load the default remote libraries, which will be reference when a remote agent is not connected.
            DefaultRemoteLibraries = new RemoteLibraries()
            {
                Functions = Functions.GetAllFunctions(),
                Connections = Connections.GetAllConnections(),
                Transforms = Transforms.GetAllTransforms()
            };

            return true;
        }

        public async Task LoadConnectionTables(DexihConnection connection, DexihRepositoryContext dbContext)
        {
            await dbContext.Entry(connection).Collection(a => a.DexihTables).Query().Where(c => c.IsValid && connection.ConnectionKey == c.ConnectionKey).LoadAsync();

            foreach (var table in connection.DexihTables.Where(c => c.HubKey == connection.HubKey))
            {
                await LoadTableColumns(table, dbContext);
            }
        }

        public async Task LoadTableColumns(DexihTable hubTable, DexihRepositoryContext dbContext)
        {
            await dbContext.Entry(hubTable).Collection(a => a.DexihTableColumns).Query().Where(c => c.IsValid && hubTable.TableKey == c.TableKey).LoadAsync();

            var columnValidationKeys = hubTable.DexihTableColumns.Where(c => c.ColumnValidationKey >= 0).Select(c => (long)c.ColumnValidationKey);
            await AddColumnValidations(columnValidationKeys, dbContext);
        }
	    
	    public void LoadTableColumns(DexihTable hubTable, DexihHub hub)
	    {
		    var columnValidationKeys = hubTable.DexihTableColumns.Where(c => c.ColumnValidationKey >= 0).Select(c => (long)c.ColumnValidationKey);
		    AddColumnValidations(columnValidationKeys, hub);
	    }

	    public async Task LoadViewDependencies(DexihView view, DexihRepositoryContext dbContext)
	    {
		    switch (view.SourceType)
		    {
			    case ESourceType.Datalink:
				    await AddDatalinks(new [] {view.SourceDatalinkKey.Value}, dbContext );
				    break;
			    case ESourceType.Table:
				    await AddTables(new[] {view.SourceTableKey.Value}, dbContext);
				    break;
		    }
	    }
	    
	    public void LoadViewDependencies(DexihView view, DexihHub hub)
	    {
		    switch (view.SourceType)
		    {
			    case ESourceType.Datalink:
				    AddDatalinks(new [] {view.SourceDatalinkKey.Value}, hub );
				    break;
			    case ESourceType.Table:
				    AddTables(new[] {view.SourceTableKey.Value}, hub);
				    break;
		    }
	    }

        public async Task LoadDatalinkDependencies(DexihDatalink hubDatalink, bool includeDependencies, DexihRepositoryContext dbContext)
        {
            if (includeDependencies)
            {
				if (hubDatalink.SourceDatalinkTable.SourceType == ESourceType.Table && hubDatalink.SourceDatalinkTable.SourceTableKey != null)
				{
					await AddTables(new[] { hubDatalink.SourceDatalinkTable.SourceTableKey.Value }, dbContext);
				}
				else if ( hubDatalink.SourceDatalinkTable.SourceDatalinkKey != null)
				{
					await AddDatalinks(new[] { hubDatalink.SourceDatalinkTable.SourceDatalinkKey.Value }, dbContext);
				}

//                if (datalink.TargetTableKey != null)
//                {
//                    await AddTables(new[] {(long)datalink.TargetTableKey}, dbContext);
//                }
                
                var tableKeys = hubDatalink.DexihDatalinkTargets.Select(c => c.TableKey);
                await AddTables(tableKeys, dbContext);

                if (hubDatalink.AuditConnectionKey != null)
                {
                    await AddConnections(new[] {hubDatalink.AuditConnectionKey.Value}, false, dbContext);
                }
            }

            await dbContext.Entry(hubDatalink).Collection(a => a.DexihDatalinkTransforms).Query().Where(a => a.IsValid && hubDatalink.DatalinkKey == a.DatalinkKey)
                .Include(c=>c.JoinDatalinkTable).OrderBy(c=>c.Position).LoadAsync();

            foreach (var datalinkTransform in hubDatalink.DexihDatalinkTransforms)
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
//                        if (item.StandardFunctionKey != null && DexihStandardFunctions.Count(c => c.StandardFunctionKey == item.StandardFunctionKey) == 0)
//                        {
//                            var standardFunction = await dbContext.DexihStandardFunctions.SingleOrDefaultAsync(c => c.StandardFunctionKey == item.StandardFunctionKey && c.IsValid);
//                            DexihStandardFunctions.Add(standardFunction);
//                        }

                        if (item.CustomFunctionKey != null)
                        {
                            var customFunction = Hub.DexihCustomFunctions.SingleOrDefault(c => c.CustomFunctionKey == item.CustomFunctionKey);
                            if (customFunction == null)
                            {
                                customFunction = await dbContext.DexihCustomFunctions.Include(c=>c.DexihCustomFunctionParameters).SingleOrDefaultAsync(c => c.CustomFunctionKey == item.CustomFunctionKey && c.IsValid);
                                Hub.DexihCustomFunctions.Add(customFunction);
                            }
                        }

                    }
                }
            }

//            if (includeDependencies)
//            {
//                foreach (var datalinkProfile in datalink.DexihDatalinkProfiles)
//                {
//                    if (DexihProfileRules.Count(c => c.ProfileRuleKey == datalinkProfile.ProfileRuleKey) == 0)
//                    {
//                        var profileRule = await dbContext.DexihProfileRules.SingleOrDefaultAsync(c => c.ProfileRuleKey == datalinkProfile.ProfileRuleKey && c.IsValid);
//                        DexihProfileRules.Add(profileRule);
//                    }
//                }
//            }
        }
	    
		public void LoadDatalinkDependencies(DexihDatalink hubDatalink, DexihHub hub)
        {
			if (hubDatalink.SourceDatalinkTable.SourceType == ESourceType.Table && hubDatalink.SourceDatalinkTable.SourceTableKey != null)
			{
				AddTables(new[] { hubDatalink.SourceDatalinkTable.SourceTableKey.Value }, hub);
			}
			else if ( hubDatalink.SourceDatalinkTable.SourceDatalinkKey != null)
			{
				AddDatalinks(new[] { hubDatalink.SourceDatalinkTable.SourceDatalinkKey.Value }, hub);
			}

//			if (datalink.TargetTableKey != null)
//			{
//				AddTables(new[] {(long)datalink.TargetTableKey}, hub);
//			}

			var tableKeys = hubDatalink.DexihDatalinkTargets.Select(c => c.TableKey);
			AddTables(tableKeys, hub);

			if (hubDatalink.AuditConnectionKey != null)
			{
				AddConnections(new[] {hubDatalink.AuditConnectionKey.Value}, false, hub);
			}
	        
			foreach (var datalinkTransform in hubDatalink.DexihDatalinkTransforms)
            {
                if (datalinkTransform.JoinDatalinkTable != null)
                {
					if (datalinkTransform.JoinDatalinkTable.SourceType == ESourceType.Table && datalinkTransform.JoinDatalinkTable.SourceTableKey != null)
					{
						AddTables(new[] { datalinkTransform.JoinDatalinkTable.SourceTableKey.Value }, hub);
					}
					else if  (datalinkTransform.JoinDatalinkTable.SourceType == ESourceType.Datalink && datalinkTransform.JoinDatalinkTable.SourceDatalinkKey != null)
					{
						AddDatalinks(new[] { datalinkTransform.JoinDatalinkTable.SourceDatalinkKey.Value }, hub);
					}
                }

                foreach (var item in datalinkTransform.DexihDatalinkTransformItems)
                {

					if (item.CustomFunctionKey != null)
					{
						var customFunction = Hub.DexihCustomFunctions.SingleOrDefault(c => c.CustomFunctionKey == item.CustomFunctionKey);
						if (customFunction == null)
						{
							customFunction = hub.DexihCustomFunctions.SingleOrDefault(c => c.CustomFunctionKey == item.CustomFunctionKey && c.IsValid);
							Hub.DexihCustomFunctions.Add(customFunction);
						}
					}
                }
            }
        }
	    
	    public void LoadDatalinkTestDependencies(DexihDatalinkTest datalinkTest, DexihHub hub)
	    {
		    var datalinkKeys = datalinkTest.DexihDatalinkTestSteps.Select(c => c.DatalinkKey);
		    AddDatalinks(datalinkKeys, hub);

		    var connectionKeys = datalinkTest.DexihDatalinkTestSteps.Select(c => c.ExpectedConnectionKey).Concat(
			    datalinkTest.DexihDatalinkTestSteps.Select(c=>c.TargetConnectionKey));
		    
		    AddConnections(connectionKeys, false, hub);
	    }
	    

        private async Task LoadDatajobDependencies(DexihDatajob hubDatajob, bool includeDependencies, DexihRepositoryContext dbContext)
        {
            await dbContext.Entry(hubDatajob).Collection(a => a.DexihTriggers).Query().Where(a => a.IsValid && hubDatajob.DatajobKey == a.DatajobKey).LoadAsync();
            await dbContext.Entry(hubDatajob).Collection(a => a.DexihDatalinkSteps).Query().Include(c=>c.DexihDatalinkStepColumns).Where(a => a.IsValid && hubDatajob.DatajobKey == a.DatajobKey).LoadAsync();

            if(includeDependencies)
            {
                if (hubDatajob.AuditConnectionKey != null)
                {
                    await AddConnections(new[] {hubDatajob.AuditConnectionKey.Value}, false, dbContext);
                }

                await AddDatalinks(hubDatajob.DexihDatalinkSteps.Select(c => c.DatalinkKey).ToArray(), dbContext);
            }
        }
	    
	    private void LoadDatajobDependencies(DexihDatajob hubDatajob, DexihHub hub)
	    {
			if (hubDatajob.AuditConnectionKey != null)
			{
				AddConnections(new[] {hubDatajob.AuditConnectionKey.Value}, false, hub);
			}

			AddDatalinks(hubDatajob.DexihDatalinkSteps.Select(c => c.DatalinkKey).ToArray(), hub);
	    }

        private async Task LoadColumnValidationDependencies(DexihColumnValidation columnValidation, DexihRepositoryContext dbContext)
        {
            if(columnValidation.LookupColumnKey != null)
            {
                var column = await dbContext.DexihTableColumns.SingleOrDefaultAsync(c => c.ColumnKey == columnValidation.LookupColumnKey);
                await AddTables(new[] { column.GetParentTableKey() }, dbContext);
            }
        }
	    
	    private void LoadColumnValidationDependencies(DexihColumnValidation columnValidation, DexihHub hub)
	    {
		    if(columnValidation.LookupColumnKey != null)
		    {
			    var column = hub.GetColumnFromKey(columnValidation.LookupColumnKey.Value);
			    AddTables(new[] { column.GetParentTableKey() }, hub);
		    }
	    }

        public async Task AddConnections(IEnumerable<long> connectionKeys, bool includeTables, DexihRepositoryContext dbContext)
        {
            foreach (var connectionKey in connectionKeys)
            {
                var connection = Hub.DexihConnections.SingleOrDefault(c => c.ConnectionKey == connectionKey);
                if(connection == null)
                {
                    connection = await dbContext.DexihConnections.SingleOrDefaultAsync(c => c.HubKey == HubKey && c.ConnectionKey == connectionKey && c.IsValid);
                    Hub.DexihConnections.Add(connection);
                }

                if(includeTables)
                {
                    await LoadConnectionTables(connection, dbContext);
                }
            }
        }

	    public void AddConnections(IEnumerable<long> connectionKeys, bool includeTables, DexihHub hub)
	    {
		    var connections = hub.DexihConnections.Where(c => connectionKeys.ToArray().Contains(c.ConnectionKey));
		    foreach (var connection in connections)
		    {
			    var existingConnection = Hub.DexihConnections.SingleOrDefault(c => c.ConnectionKey == connection.ConnectionKey);
			    if(existingConnection == null)
			    {
				    if (includeTables)
				    {
					    Hub.DexihConnections.Add(connection);
				    }
				    else
				    {
					    Hub.DexihConnections.Add(connection.CloneProperties<DexihConnection>(true));
				    }
			    }
		    }
	    }

        public async Task AddDatalinks(IEnumerable<long> datalinkKeys, DexihRepositoryContext dbContext)
        {
            var datalinks = await GetDatalinks(datalinkKeys, dbContext);
            
            foreach (var datalink in datalinks)
            {
                await LoadDatalinkDependencies(datalink, true, dbContext);
                Hub.DexihDatalinks.Add(datalink);
            }
        }

	    public void AddDatalinks(IEnumerable<long> datalinkKeys, DexihHub hub)
	    {
		    var datalinks = hub.DexihDatalinks.Where(c => datalinkKeys.ToArray().Contains(c.DatalinkKey));
		    foreach (var datalink in datalinks)
		    {
			    var existingDatalink = Hub.DexihDatalinks.SingleOrDefault(c => c.DatalinkKey == datalink.DatalinkKey);
			    if(existingDatalink == null)
			    {
				    Hub.DexihDatalinks.Add(datalink);
				    LoadDatalinkDependencies(datalink, hub);
			    }
		    }
	    }

	    public void AddDatalinkTests(IEnumerable<long> datalinkTestKeys, DexihHub hub)
	    {
		    var datalinkTests = hub.DexihDatalinkTests.Where(c => datalinkTestKeys.ToArray().Contains(c.DatalinkTestKey));
		    foreach (var datalinkTest in datalinkTests)
		    {
			    var existingDatalinkTest = Hub.DexihDatalinkTests.SingleOrDefault(c => c.DatalinkTestKey == datalinkTest.DatalinkTestKey);
			    if(existingDatalinkTest == null)
			    {
				    Hub.DexihDatalinkTests.Add(datalinkTest);
				    LoadDatalinkTestDependencies(datalinkTest, hub);
			    }
		    }
	    }

        public async Task AddTables(IEnumerable<long> tableKeys, DexihRepositoryContext dbContext)
        {
            foreach (var tableKey in tableKeys)
            {
                DexihTable hubTable = null;
                foreach(var connection in Hub.DexihConnections)
                {
                    hubTable = connection.DexihTables.SingleOrDefault(c => c.TableKey == tableKey);
                    if (hubTable != null) break;
                }

                if(hubTable == null)
                {
                    hubTable = await dbContext.DexihTables.SingleOrDefaultAsync(c => c.HubKey == HubKey && c.TableKey == tableKey && c.IsValid);
	                if (hubTable == null)
	                {
		                throw new CacheManagerException($"Could not find the table with the key {tableKey}.");
	                }
                    await LoadTableColumns(hubTable, dbContext);

					await AddConnections(new long[] { hubTable.ConnectionKey }, false, dbContext);
					var connection = Hub.DexihConnections.SingleOrDefault(c => c.ConnectionKey == hubTable.ConnectionKey);

					connection.DexihTables.Add(hubTable);
	                
	                if(hubTable.FileFormatKey != null)
	                {
		                var fileFormat = Hub.DexihFileFormats.SingleOrDefault(c => c.FileFormatKey == hubTable.FileFormatKey);
		                if(fileFormat == null)
		                {
			                var internalHub = await dbContext.DexihHubs.FirstAsync(c => c.IsValid);
			                fileFormat = await dbContext.DexihFileFormat.SingleOrDefaultAsync(c => (c.HubKey == HubKey || c.HubKey == internalHub.HubKey) && c.FileFormatKey == hubTable.FileFormatKey && c.IsValid);
			                Hub.DexihFileFormats.Add(fileFormat);
		                }
	                }
	                await LoadTableColumns(hubTable, dbContext);
                }
            }
        }
	    
	    public void AddTables(IEnumerable<long> tableKeys, DexihHub hub)
	    {
		    foreach (var tableKey in tableKeys)
		    {
			    DexihTable hubTable = null;
			    // table already added to cache?
			    foreach(var connection in Hub.DexihConnections)
			    {
				    hubTable = connection.DexihTables.SingleOrDefault(c => c.TableKey == tableKey);
				    if (hubTable != null) break;
			    }

			    // not added, then add table to cache.
			    if(hubTable == null)
			    {
				    hubTable = hub.GetTableFromKey(tableKey);

				    if (hubTable == null)
				    {
					    throw new CacheManagerException($"Could not find the table with the key {tableKey}.");
				    }

				    LoadTableColumns(hubTable, hub);
				    
				    AddConnections(new long[] { hubTable.ConnectionKey }, false, hub);
				    var connection = Hub.DexihConnections.SingleOrDefault(c => c.ConnectionKey == hubTable.ConnectionKey);
				    connection.DexihTables.Add(hubTable);
				    
				    if(hubTable.FileFormatKey != null)
				    {
					    var fileFormat = Hub.DexihFileFormats.SingleOrDefault(c => c.FileFormatKey == hubTable.FileFormatKey);
					    if(fileFormat == null)
					    {
						    fileFormat = hub.DexihFileFormats.SingleOrDefault(c => c.FileFormatKey == hubTable.FileFormatKey && c.IsValid);
						    Hub.DexihFileFormats.Add(fileFormat);
					    }
				    }
			    }
		    }
	    }

	    /// <summary>
	    /// Adds a table object to the cache along with any dependencies.  This is used where the table has not been saved to the repository.
	    /// Ensure the tableKey is unique.
	    /// </summary>
	    /// <param name="hubTable"></param>
	    /// <param name="hubHub"></param>
	    public void AddTable(DexihTable hubTable, DexihHub hub)
	    {
		    AddConnections(new long[] { hubTable.ConnectionKey }, false, hub);
		    var connection = Hub.DexihConnections.SingleOrDefault(c => c.ConnectionKey == hubTable.ConnectionKey);
		    connection.DexihTables.Add(hubTable);
				    
		    if(hubTable.FileFormatKey != null)
		    {
			    var fileFormat = Hub.DexihFileFormats.SingleOrDefault(c => c.FileFormatKey == hubTable.FileFormatKey);
			    if(fileFormat == null)
			    {
				    fileFormat = hub.DexihFileFormats.SingleOrDefault(c => c.FileFormatKey == hubTable.FileFormatKey && c.IsValid);
				    Hub.DexihFileFormats.Add(fileFormat);
			    }
		    }
	    }

	    /// <summary>
	    /// Adds a datalink to the cache, along with any dependencies.
	    /// </summary>
	    /// <param name="hubDatalink"></param>
	    /// <param name="hubHub"></param>
	    public void AddDatalink(DexihDatalink hubDatalink, DexihHub hub)
	    {
		    if (hubDatalink.SourceDatalinkTable.SourceType == ESourceType.Table && hubDatalink.SourceDatalinkTable.SourceTableKey != null)
		    {
			    AddTables(new[] { hubDatalink.SourceDatalinkTable.SourceTableKey.Value }, hub);
		    }
		    else if ( hubDatalink.SourceDatalinkTable.SourceDatalinkKey != null)
		    {
			    AddDatalinks(new[] { hubDatalink.SourceDatalinkTable.SourceDatalinkKey.Value }, hub);
		    }

//		    if (datalink.TargetTableKey != null)
//		    {
//			    AddTables(new[] {(long)datalink.TargetTableKey}, hub);
//		    }

		    if (hubDatalink.AuditConnectionKey != null)
		    {
			    AddConnections(new[] {hubDatalink.AuditConnectionKey.Value}, false, hub);
		    }

		    var tableKeys = hubDatalink.DexihDatalinkTargets.Select(c => c.TableKey);
		    AddTables(tableKeys, hub);
		    
		    
		    foreach (var datalinkTransform in hubDatalink.DexihDatalinkTransforms)
		    {
			    if (datalinkTransform.JoinDatalinkTable != null)
			    {
				    if (datalinkTransform.JoinDatalinkTable.SourceType == ESourceType.Table && datalinkTransform.JoinDatalinkTable.SourceTableKey != null)
				    {
					    AddTables(new[] { datalinkTransform.JoinDatalinkTable.SourceTableKey.Value }, hub);
				    }
				    else if ( datalinkTransform.JoinDatalinkTable.SourceDatalinkKey != null)
				    {
					    AddDatalinks(new[] { datalinkTransform.JoinDatalinkTable.SourceDatalinkKey.Value }, hub);
				    }
			    }

			    foreach (var item in datalinkTransform.DexihDatalinkTransformItems)
			    {
				    if (item.CustomFunctionKey != null)
				    {
					    AddCustomFunctions(new[] {(long)item.CustomFunctionKey}, hub);
				    }
			    }
		    }
		    
		    Hub.DexihDatalinks.Add(hubDatalink);
	    }

        public async Task AddDatajobs(IEnumerable<long> datajobKeys, DexihRepositoryContext dbContext)
        {
            foreach (var datajobKey in datajobKeys)
            {
                var existingJob = Hub.DexihDatajobs.SingleOrDefault(c=>c.DatajobKey == datajobKey);
                if(existingJob == null)
                {
                    var job = await dbContext.DexihDatajobs.SingleOrDefaultAsync(c => c.HubKey == HubKey && c.DatajobKey == datajobKey && c.IsValid);
                    Hub.DexihDatajobs.Add(job);
	                await LoadDatajobDependencies(job, true, dbContext);
                }
            }
        }
	    
	    public void AddDatajobs(IEnumerable<long> datajobKeys, DexihHub hub)
	    {
		    foreach (var datajobKey in datajobKeys)
		    {
			    var existingJob = Hub.DexihDatajobs.SingleOrDefault(c=>c.DatajobKey == datajobKey);
			    if(existingJob == null)
			    {
				    var job = hub.DexihDatajobs.SingleOrDefault(c => c.HubKey == HubKey && c.DatajobKey == datajobKey && c.IsValid);
				    Hub.DexihDatajobs.Add(job);
				    LoadDatajobDependencies(job, hub);
			    }

		    }
	    }

	    public async Task AddColumnValidations(IEnumerable<long> columnValidationKeys, DexihRepositoryContext dbContext)
	    {
		    foreach (var columnValidationKey in columnValidationKeys)
		    {
			    var columnValidation = Hub.DexihColumnValidations.SingleOrDefault(c => c.ColumnValidationKey == columnValidationKey);
			    if(columnValidation == null)
			    {
				    columnValidation = await dbContext.DexihColumnValidation.SingleOrDefaultAsync(c => c.HubKey == HubKey && c.ColumnValidationKey == columnValidationKey && c.IsValid);
				    Hub.DexihColumnValidations.Add(columnValidation);
				    await LoadColumnValidationDependencies(columnValidation, dbContext);
			    }
		    }
	    }
	    
	    public void AddColumnValidations(IEnumerable<long> columnValidationKeys, DexihHub hub)
	    {
		    foreach (var columnValidationKey in columnValidationKeys)
		    {
			    var columnValidation = Hub.DexihColumnValidations.SingleOrDefault(c => c.ColumnValidationKey == columnValidationKey);
			    if(columnValidation == null)
			    {
				    columnValidation = hub.DexihColumnValidations.SingleOrDefault(c => c.HubKey == HubKey && c.ColumnValidationKey == columnValidationKey && c.IsValid);
				    Hub.DexihColumnValidations.Add(columnValidation);
				    LoadColumnValidationDependencies(columnValidation, hub);
			    }
		    }
	    }
        
        public async Task AddCustomFunctions(IEnumerable<long> customFunctionKeys, DexihRepositoryContext dbContext)
        {
            foreach (var customFunctionKey in customFunctionKeys)
            {
                var customFunction = Hub.DexihCustomFunctions.SingleOrDefault(c => c.CustomFunctionKey == customFunctionKey);
                if(customFunction == null)
                {
                    customFunction = await dbContext.DexihCustomFunctions.SingleOrDefaultAsync(c => c.HubKey == HubKey && c.CustomFunctionKey == customFunctionKey && c.IsValid);
                    Hub.DexihCustomFunctions.Add(customFunction);
                }
            }
        }

	    public void AddCustomFunctions(IEnumerable<long> customFunctionKeys, DexihHub hub)
	    {
		    foreach (var customFunctionKey in customFunctionKeys)
		    {
			    var customFunction = Hub.DexihCustomFunctions.SingleOrDefault(c => c.CustomFunctionKey == customFunctionKey);
			    if(customFunction == null)
			    {
				    customFunction = hub.DexihCustomFunctions.SingleOrDefault(c => c.HubKey == HubKey && c.CustomFunctionKey == customFunctionKey && c.IsValid);
				    Hub.DexihCustomFunctions.Add(customFunction);
			    }
		    }
	    }
	    
		public async Task<DexihDatalink> GetDatalink(long datalinkKey, DexihRepositoryContext dbContext)
        {
	        var datalinks = await GetDatalinks(new[] {datalinkKey}, dbContext);

	        if (datalinks.Any())
	        {
		        return datalinks[0];
	        }

	        throw new RepositoryManagerException($"The datalink the key {datalinkKey} could not be found.");
        }
		
		public async Task<DexihDatalink[]> GetDatalinks(IEnumerable<long> datalinkKeys, DexihRepositoryContext dbContext)
		{
            try
            {
	            var datalinks = await dbContext.DexihDatalinks
		            .Where(c => c.IsValid && c.HubKey == HubKey && datalinkKeys.Contains(c.DatalinkKey))
		            .ToArrayAsync();

	            var datalinkTableKeys = datalinks.Select(c => c.SourceDatalinkTableKey).ToList();

	            await dbContext.DexihDatalinkProfiles
		            // .Include(c => c.ProfileRule)
		            .Where(c => c.IsValid && c.Datalink.IsValid && c.Datalink.HubKey == HubKey && datalinkKeys.Contains(c.DatalinkKey))
		            .LoadAsync();

	            await dbContext.DexihDatalinkTargets
		            .Where(c => c.IsValid && c.HubKey == HubKey && datalinkKeys.Contains(c.DatalinkKey))
		            .ToArrayAsync();
		            
	            var transforms = await dbContext.DexihDatalinkTransforms
		            .Where(c => c.IsValid && c.HubKey == HubKey && datalinkKeys.Contains(c.DatalinkKey))
		            .ToArrayAsync();

	            var datalinkTransformKeys = transforms.Select(c => c.DatalinkTransformKey);

	            var transformItems = await dbContext.DexihDatalinkTransformItems
		            // .Include(c => c.StandardFunction)
		            .Where(c => c.IsValid && c.HubKey == HubKey && datalinkTransformKeys.Contains(c.DatalinkTransformKey))
		            .OrderBy(c => c.Dt.Datalink.HubKey).ThenBy(c => c.Dt.DatalinkTransformKey)
		            .ThenBy(c => c.DatalinkTransformItemKey)
		            .ToArrayAsync();

                var transformItemKeys = transformItems.Select(c => c.DatalinkTransformItemKey);

	            var parameters = await dbContext.DexihFunctionParameters
		            .Where(c => c.IsValid && c.HubKey == HubKey && transformItemKeys.Contains(c.DatalinkTransformItemKey))
		            .OrderBy(c => c.DtItem.Dt.Datalink.HubKey).ThenBy(c => c.DtItem.Dt.DatalinkTransformKey).ThenBy(c => c.DtItem.DatalinkTransformItemKey).ThenBy(c => c.Position)
		            .ToArrayAsync();

	            var parameterKeys = parameters.Select(c => c.FunctionParameterKey);
	            var parameterArrays = await dbContext.DexihFunctionArrayParameters
		            .Where(c => c.IsValid && c.HubKey == HubKey && parameterKeys.Contains(c.FunctionParameterKey))
		            .OrderBy(c => c.FunctionParameter.DtItem.Dt.Datalink.HubKey).ThenBy(c => c.FunctionParameter.DtItem.Dt.DatalinkTransformKey).ThenBy(c => c.FunctionParameter.DtItem.DatalinkTransformItemKey).ThenBy(c => c.Position)
		            .ToArrayAsync();

	            datalinkTableKeys.AddRange(transforms.Where(c => c.JoinDatalinkTableKey != null).Select(c => c.JoinDatalinkTableKey.Value));

	            if (datalinkTableKeys.Any())
	            {
		            await dbContext.DexihDatalinkTables
			            .Where(c => c.IsValid && c.HubKey == HubKey && datalinkTableKeys.Contains(c.DatalinkTableKey))
			            .LoadAsync();

		            await dbContext.DexihDatalinkColumns
			            .Where(c => c.IsValid && c.HubKey == HubKey && c.DatalinkTableKey != null && datalinkTableKeys.Contains((long)c.DatalinkTableKey))
			            .LoadAsync();
	            }

	            var datalinkColumnKeys = transformItems.Where(c => c.SourceDatalinkColumnKey != null).Select(c => c.SourceDatalinkColumnKey).ToList();
	            datalinkColumnKeys.AddRange(transformItems.Where(c => c.TargetDatalinkColumnKey != null).Select(c => c.TargetDatalinkColumnKey));
	            datalinkColumnKeys.AddRange(parameters.Where(c => c.DatalinkColumnKey != null).Select(c => c.DatalinkColumnKey));
	            datalinkColumnKeys.AddRange(parameterArrays.Where(c => c.DatalinkColumnKey != null).Select(c => c.DatalinkColumnKey));

	            if (datalinkColumnKeys.Any())
	            {
		            await dbContext.DexihDatalinkColumns
			            .Where(c => c.IsValid && c.HubKey == HubKey && datalinkColumnKeys.Contains(c.DatalinkColumnKey))
			            .LoadAsync();
	            }

	            return datalinks;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Get datalinks with keys {string.Join(", ", datalinkKeys)} failed.  {ex.Message}", ex);
            }

        }

	    public async Task<DexihDatalinkTest> GetDatalinkTest(long datalinkTestKey, DexihRepositoryContext dbContext)
	    {
		    var datalinkTest = await dbContext.DexihDatalinkTests.Include(c => c.DexihDatalinkTestSteps)
			    .ThenInclude(d => d.DexihDatalinkTestTables).SingleOrDefaultAsync(c=>c.DatalinkTestKey == datalinkTestKey);

		    return datalinkTest;
	    }
    }
}

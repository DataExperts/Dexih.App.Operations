using dexih.functions;
using dexih.repository;
using dexih.transforms;
using dexih.transforms.Transforms;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using dexih.operations.Extensions;
using Dexih.Utils.CopyProperties;

using Microsoft.Extensions.Logging;

namespace dexih.operations
{
    [DataContract]
    public class CacheManager
    {
        [DataMember(Order = 0)]
        public DexihHub Hub { get; set; }

        [DataMember(Order = 1)]
        public string BuildVersion { get; set; }

        [DataMember(Order = 2)]
        public DateTime BuildDate { get; set; }

        [DataMember(Order = 3)]
        public string GoogleClientId { get; set; }

        [DataMember(Order = 4)]
        public string MicrosoftClientId { get; set; }

        [DataMember(Order = 5)]
        public string GoogleMapsAPIKey { get; set; }
        
        [DataMember(Order = 6)]
        public RemoteLibraries DefaultRemoteLibraries { get; set; }

        private readonly ILogger _logger;

		public CacheManager()
		{
		}

        public CacheManager(long hubKey, string cacheEncryptionKey, ILogger logger = null)
        {
	        _logger = logger;
            HubKey = hubKey;
            CacheEncryptionKey = cacheEncryptionKey;
            Initialize(hubKey);
        }
        
        private void Initialize(long hubKey)
        {
            Hub = new DexihHub() { HubKey = hubKey };
		}

        
		[DataMember(Order = 7)]
        public long HubKey { get; set; }
        
	    /// <summary>
        /// Key used to encrypt cache fields (such as passwords) when they are saved to repository or json file.
        /// </summary>
        [DataMember(Order = 8)]
        public string CacheEncryptionKey { get; set; }

        public async Task<DexihHub> InitHub(DexihRepositoryContext dbContext)
        {
            Hub = await dbContext.DexihHubs.SingleOrDefaultAsync(c => c.IsValid && c.HubKey == HubKey && c.IsValid);

	        if (Hub == null)
	        {
		        throw new CacheManagerException($"The hub with the key {HubKey} no longer exists.");
	        }
	        
            return Hub;
        }
        
		
	    public async Task<DexihHub> LoadHub(DexihRepositoryContext dbContext)
        {
			try
			{
				var stopWatch = Stopwatch.StartNew();
				
                await InitHub(dbContext);

				if (Hub == null)
				{
                    throw new CacheManagerException($"The hub with the key {HubKey} could not be found in the repository.");
				}
				
                var variables = dbContext.DexihHubVariables
                    .Where(c => c.IsValid && c.HubKey == HubKey);

                Hub.DexihHubVariables = await variables.ToHashSetAsync();
                
                Hub.DexihConnections = await dbContext.DexihConnections
					.Where(c => c.IsValid && c.HubKey == HubKey).ToHashSetAsync();

                await dbContext.DexihTableColumns
	                .Where(c => c.IsValid && c.HubKey == HubKey)
	                .LoadAsync();

                Hub.DexihTables = await dbContext.DexihTables
	                .Where(c => c.IsValid && c.HubKey == HubKey)
	                .ToHashSetAsync();

                // load the datalinks
                var datalinks = dbContext.DexihDatalinks
                    .Where(c => c.IsValid && c.HubKey == HubKey);

                await dbContext.DexihDatalinkParameters
	                .Where(c => c.IsValid && c.HubKey == HubKey)
	                .LoadAsync();
                
                await dbContext.DexihDatalinkTargets
	                .Where(c => c.IsValid && c.HubKey == HubKey)
	                .LoadAsync();

				await dbContext.DexihDatalinkTransforms
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();

				await dbContext.DexihDatalinkTransformItems
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .OrderBy(c => c.Dt.Datalink.HubKey).ThenBy(c => c.Dt.Key).ThenBy(c => c.Key)
					.LoadAsync();

				await dbContext.DexihFunctionParameters
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .OrderBy(c => c.DtItem.Dt.Datalink.HubKey).ThenBy(c => c.DtItem.Dt.Key).ThenBy(c => c.DtItem.Key).ThenBy(c => c.Position)
					.LoadAsync();

				await dbContext.DexihFunctionArrayParameters
					.Where(c => c.IsValid && c.HubKey == HubKey)
					.OrderBy(c => c.FunctionParameter.DtItem.Dt.Datalink.HubKey).ThenBy(c => c.FunctionParameter.DtItem.Dt.Key).ThenBy(c => c.FunctionParameter.DtItem.Key).ThenBy(c => c.Position)
					.LoadAsync();

				await dbContext.DexihDatalinkTables
					.Where(c => c.IsValid && c.HubKey == HubKey)
					.LoadAsync();

				await dbContext.DexihDatalinkColumns
					.Where(c => c.IsValid && c.HubKey == HubKey)
					.LoadAsync();
				
				await dbContext.DexihDatalinkProfiles
					.Where(c => c.IsValid && c.Datalink.IsValid && c.Datalink.HubKey == HubKey)
					.LoadAsync();

                Hub.DexihDatalinks = await datalinks.ToHashSetAsync();

				// load the datajobs with all dependent schedules/datalink steps.
				var datajobs = dbContext.DexihDatajobs
					.Where(c => c.IsValid && c.HubKey == HubKey);

				await dbContext.DexihDatajobParameters
					.Where(c => c.IsValid && c.HubKey == HubKey)
					.LoadAsync();
				
				await dbContext.DexihDatalinkStep
                    .Where(c => c.IsValid && c.HubKey == HubKey)
                    .LoadAsync();
				
				await dbContext.DexihDatalinkStepParameters
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
					.ToHashSetAsync();

				await dbContext.DexihDatalinkTestSteps
					.Where(c => c.IsValid && c.HubKey == HubKey)
					.LoadAsync();

				await dbContext.DexihDatalinkTestTables
					.Where(c => c.IsValid && c.HubKey == HubKey)
					.LoadAsync();
				
				Hub.DexihDatajobs = await datajobs.ToHashSetAsync();

				var dashboards = dbContext.DexihDashboards
					.Where(c => c.IsValid && c.HubKey == HubKey);
				
				await dbContext.DexihDashboardItems
					.Where(c => c.IsValid && c.HubKey == HubKey)
					.LoadAsync();

				await dbContext.DexihDashboardParameters
					.Where(c => c.IsValid && c.HubKey == HubKey)
					.LoadAsync();

				await dbContext.DexihDashboardItemParameters
					.Where(c => c.IsValid && c.HubKey == HubKey)
					.LoadAsync();
				
				Hub.DexihDashboards = await dashboards.ToHashSetAsync();

				await dbContext.DexihViewParameters
					.Where(c => c.IsValid && c.HubKey == HubKey)
					.LoadAsync();

				Hub.DexihViews = await dbContext.DexihViews.Where(c => c.HubKey == HubKey && c.IsValid).ToHashSetAsync();
				
				await dbContext.DexihApiParameters
					.Where(c => c.IsValid && c.HubKey == HubKey)
					.LoadAsync();
				
				Hub.DexihApis = await dbContext.DexihApis.Where(c => c.HubKey == HubKey && c.IsValid).ToHashSetAsync();

				await dbContext.DexihCustomFunctionParameters
					.Where(c => c.IsValid && c.HubKey == HubKey)
					.LoadAsync();
				
				Hub.DexihCustomFunctions = await dbContext.DexihCustomFunctions.Include(c=>c.DexihCustomFunctionParameters).Where(c => c.HubKey == HubKey && c.IsValid).ToHashSetAsync();

				Hub.DexihFileFormats = await dbContext.DexihFileFormats.Where(c => (c.HubKey == HubKey) && c.IsValid).ToHashSetAsync();
				Hub.DexihColumnValidations = await dbContext.DexihColumnValidations.Where(c => c.HubKey == HubKey && c.IsValid).ToHashSetAsync();
			    Hub.DexihRemoteAgentHubs = await dbContext.DexihRemoteAgentHubs.Where(c => c.HubKey == HubKey && c.IsValid).ToHashSetAsync();
			    Hub.DexihListOfValues = await dbContext.DexihListOfValues.Where(c => (c.HubKey == HubKey) && c.IsValid).ToHashSetAsync();
			    
				_logger?.LogTrace($"Load hub name {Hub.Name} took {stopWatch.ElapsedMilliseconds}ms.");

				return Hub;
			} catch(Exception ex)
			{
                throw new CacheManagerException($"An error occurred trying to load the hub.  {ex.Message}", ex);
			}
        }

        public bool LoadGlobal(string version, DateTime buildDate)
        {
            BuildVersion = version;
            BuildDate = buildDate;

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
	        var tables = dbContext.DexihTables
		        .Where(c => c.HubKey == HubKey && c.ConnectionKey == connection.Key && c.IsValid).AsAsyncEnumerable();

            await foreach (var table in tables)
            {
                await LoadTableColumns(table, dbContext);
                Hub.DexihTables.Add(table);
            }
        }

        public async Task LoadTableColumns(DexihTable hubTable, DexihRepositoryContext dbContext)
        {
            await dbContext.Entry(hubTable).Collection(a => a.DexihTableColumns).Query().Where(c => c.IsValid && hubTable.Key == c.TableKey).LoadAsync();

            var columnValidationKeys = hubTable.DexihTableColumns.Where(c => c.ColumnValidationKey >= 0).Select(c => (long)c.ColumnValidationKey);
            await AddColumnValidations(columnValidationKeys, dbContext);
        }
	    
	    public void LoadTableColumns(DexihTable hubTable, DexihHub hub)
	    {
		    var columnValidationKeys = hubTable.DexihTableColumns.Where(c => c.ColumnValidationKey >= 0).Select(c => (long)c.ColumnValidationKey);
		    AddColumnValidations(columnValidationKeys, hub);
	    }

	    public async Task LoadTableDependencies(DexihTable table, DexihRepositoryContext dbContext)
	    {
		    await AddConnections(new [] {table.ConnectionKey}, false, dbContext);

		    var columnValidationKeys = table.DexihTableColumns.Where(c => c.ColumnValidationKey >= 0).Select(c => (long)c.ColumnValidationKey);
		    await AddColumnValidations(columnValidationKeys, dbContext);
		    
		    if (table.FileFormatKey != null)
		    {
			    await AddFileFormats(new[] {table.FileFormatKey.Value}, dbContext);
		    }
	    }
	    
	    public void LoadTableDependencies(DexihTable table, DexihHub hub)
	    {
		    AddConnections(new [] {table.ConnectionKey}, false, hub);

		    var columnValidationKeys = table.DexihTableColumns.Where(c => c.ColumnValidationKey >= 0).Select(c => (long)c.ColumnValidationKey);
		    AddColumnValidations(columnValidationKeys, hub);

		    if (table.FileFormatKey != null)
		    {
			    AddFileFormats(new[] {table.FileFormatKey.Value}, hub);
		    }
	    }

	    public async Task LoadListOfValuesDependencies(DexihListOfValues listOfValues, DexihRepositoryContext dbContext)
	    {
		    switch (listOfValues.SourceType)
		    {
			    case ELOVObjectType.Datalink:
				    if (listOfValues.SourceDatalinkKey == null)
				    {
					    throw new Exception($"The list of values {listOfValues.Name} does not contain a linked datalink.");
				    }
				    await AddDatalinks(new [] {listOfValues.SourceDatalinkKey.Value}, dbContext );
				    break;
			    case ELOVObjectType.Table:
				    if (listOfValues.SourceTableKey == null)
				    {
					    throw new Exception($"The list of values {listOfValues.Name} does not contain a linked table.");
				    }
				    await AddTables(new[] {listOfValues.SourceTableKey.Value}, dbContext);
				    break;
		    }
	    }
	    
	    public void LoadListOfValuesDependencies(DexihListOfValues listOfValues, DexihHub hub)
	    {
		    switch (listOfValues.SourceType)
		    {
			    case ELOVObjectType.Datalink:
				    if (listOfValues.SourceDatalinkKey == null)
				    {
					    throw new Exception($"The list of values {listOfValues.Name} does not contain a linked datalink.");
				    }
				    AddDatalinks(new [] {listOfValues.SourceDatalinkKey.Value}, hub );
				    break;
			    case ELOVObjectType.Table:
				    if (listOfValues.SourceTableKey == null)
				    {
					    throw new Exception($"The list of values {listOfValues.Name} does not contain a linked table.");
				    }
				    AddTables(new[] {listOfValues.SourceTableKey.Value}, hub);
				    break;
		    }
	    }

	    public void LoadParametersDependencies(IEnumerable<InputParameterBase> parameters, DexihHub hub)
	    {
		    foreach (var parameter in parameters.Where(c => c.ListOfValues != null))
		    {
			    LoadListOfValuesDependencies(parameter.ListOfValues, hub);
		    }
	    }

	    public async Task LoadParametersDependencies(IEnumerable<InputParameterBase> parameters, DexihRepositoryContext dbContext)
	    {
		    foreach (var parameter in parameters)
		    {
			    await LoadListOfValuesDependencies(parameter.ListOfValues, dbContext);
		    }
	    }
	    public async Task LoadViewDependencies(DexihView view, DexihRepositoryContext dbContext)
	    {
		    await dbContext.Entry(view)
			    .Collection(a => a.Parameters)
			    .Query()
			    .Where(c => c.IsValid && view.Key == c.ViewKey)
			    .AsNoTracking().LoadAsync();

		    await LoadParametersDependencies(view.Parameters, dbContext);

		    switch (view.SourceType)
		    {
			    case EDataObjectType.Datalink:
				    if (view.SourceDatalinkKey == null)
				    {
					    throw new Exception($"The view {view.Name} does not contain a linked datalink.");
				    }
				    await AddDatalinks(new [] {view.SourceDatalinkKey.Value}, dbContext );
				    break;
			    case EDataObjectType.Table:
				    if (view.SourceTableKey == null)
				    {
					    throw new Exception($"The view {view.Name} does not contain a linked table.");
				    }
				    await AddTables(new[] {view.SourceTableKey.Value}, dbContext);
				    break;
		    }
	    }
	    
	    public void LoadViewDependencies(DexihView view, DexihHub hub)
	    {
		    LoadParametersDependencies(view.Parameters, hub);

		    switch (view.SourceType)
		    {
			    case EDataObjectType.Datalink:
				    if (view.SourceDatalinkKey == null)
				    {
					    throw new Exception($"The view {view.Name} does not contain a linked datalink.");
				    }
				    AddDatalinks(new [] {view.SourceDatalinkKey.Value}, hub );
				    break;
			    case EDataObjectType.Table:
				    if (view.SourceTableKey == null)
				    {
					    throw new Exception($"The view {view.Name} does not contain a linked table.");
				    }
				    AddTables(new[] {view.SourceTableKey.Value}, hub);
				    break;
		    }
	    }
	    
	    public async Task LoadDashboardDependencies(DexihDashboard dashboard, DexihRepositoryContext dbContext)
	    {
		    await dbContext.Entry(dashboard).Collection(a => a.Parameters).Query().Where(c => c.IsValid && dashboard.Key == c.DashboardKey)
			    .AsNoTracking().LoadAsync();

		    await LoadParametersDependencies(dashboard.Parameters, dbContext);

		    await dbContext.Entry(dashboard).Collection(a => a.DexihDashboardItems).Query().Include(c => c.Parameters).Where(c => c.IsValid && dashboard.Key == c.DashboardKey).AsNoTracking().LoadAsync();
		    await AddViews(dashboard.DexihDashboardItems.Select(c => c.ViewKey), dbContext);
	    }
	    
	    public void LoadDashboardDependencies(DexihDashboard dashboard, DexihHub hub)
	    {
		    LoadParametersDependencies(dashboard.Parameters, hub);

		    AddViews(dashboard.DexihDashboardItems.Select(c => c.ViewKey), hub);
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

            await dbContext.Entry(hubDatalink).Collection(a => a.Parameters).Query()
	            .Where(a => a.IsValid && hubDatalink.Key == a.DatalinkKey).LoadAsync();
            
            await LoadParametersDependencies(hubDatalink.Parameters, dbContext);

            await dbContext.Entry(hubDatalink).Collection(a => a.DexihDatalinkTransforms).Query().Where(a => a.IsValid && hubDatalink.Key == a.DatalinkKey)
                .Include(c=>c.JoinDatalinkTable).OrderBy(c=>c.Position).LoadAsync();

            foreach (var datalinkTransform in hubDatalink.DexihDatalinkTransforms)
            {
                if (includeDependencies && datalinkTransform.JoinDatalinkTable != null)
                {
                    await dbContext.Entry(datalinkTransform.JoinDatalinkTable).Collection(a => a.DexihDatalinkColumns).Query().Where(a => a.IsValid && datalinkTransform.JoinDatalinkTable.Key == a.DatalinkTableKey).OrderBy(c=>c.Position).LoadAsync();
                    
					if (datalinkTransform.JoinDatalinkTable.SourceType == ESourceType.Table && datalinkTransform.JoinDatalinkTable.SourceTableKey != null)
					{
						await AddTables(new[] { datalinkTransform.JoinDatalinkTable.SourceTableKey.Value }, dbContext);
					}
					else if  (datalinkTransform.JoinDatalinkTable.SourceType == ESourceType.Datalink && datalinkTransform.JoinDatalinkTable.SourceDatalinkKey != null)
					{
						await AddDatalinks(new[] { datalinkTransform.JoinDatalinkTable.SourceDatalinkKey.Value }, dbContext);
					}
                }

                await dbContext.Entry(datalinkTransform).Collection(a => a.DexihDatalinkTransformItems).Query().Where(a => a.IsValid && datalinkTransform.Key == a.DatalinkTransformKey).Include(c => c.TargetDatalinkColumn).OrderBy(c=>c.Position).LoadAsync();
                foreach (var item in datalinkTransform.DexihDatalinkTransformItems)
                {
                    await dbContext.Entry(item).Collection(a => a.DexihFunctionParameters).Query().Where(a => a.IsValid && item.Key == a.DatalinkTransformItemKey).Include(c=>c.DatalinkColumn).OrderBy(c=>c.Position).LoadAsync();

                    if (includeDependencies)
                    {
//                        if (item.StandardFunctionKey != null && DexihStandardFunctions.Count(c => c.StandardFunctionKey == item.StandardFunctionKey) == 0)
//                        {
//                            var standardFunction = await dbContext.DexihStandardFunctions.SingleOrDefaultAsync(c => c.StandardFunctionKey == item.StandardFunctionKey && c.IsValid);
//                            DexihStandardFunctions.Add(standardFunction);
//                        }

                        if (item.CustomFunctionKey != null)
                        {
                            var customFunction = Hub.DexihCustomFunctions.SingleOrDefault(c => c.IsValid && c.Key == item.CustomFunctionKey);
                            if (customFunction == null)
                            {
                                customFunction = await dbContext.DexihCustomFunctions.Include(c=>c.DexihCustomFunctionParameters).SingleOrDefaultAsync(c => c.Key == item.CustomFunctionKey && c.IsValid);
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
			
			LoadParametersDependencies(hubDatalink.Parameters, hub);
	        
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
						var customFunction = Hub.DexihCustomFunctions.SingleOrDefault(c => c.IsValid && c.Key == item.CustomFunctionKey);
						if (customFunction == null)
						{
							customFunction = hub.DexihCustomFunctions.SingleOrDefault(c => c.IsValid && c.Key == item.CustomFunctionKey && c.IsValid);
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
			    datalinkTest.DexihDatalinkTestSteps.Select(c=>c.TargetConnectionKey).Concat(
				datalinkTest.DexihDatalinkTestSteps.Select(c => c.ErrorConnectionKey)));
		    
		    AddConnections(connectionKeys, false, hub);
	    }

	    public async Task LoadDatajobDependencies(DexihDatajob hubDatajob, bool includeDependencies, DexihRepositoryContext dbContext)
        {
            await dbContext.Entry(hubDatajob).Collection(a => a.DexihTriggers).Query().Where(a => a.IsValid && hubDatajob.Key == a.DatajobKey).LoadAsync();
            await dbContext.Entry(hubDatajob).Collection(a => a.DexihDatalinkSteps).Query().Include(c=> c.Parameters).Include(c=>c.DexihDatalinkStepColumns).Where(a => a.IsValid && hubDatajob.Key == a.DatajobKey).LoadAsync();
            await dbContext.Entry(hubDatajob).Collection(a => a.Parameters).Query().Where(c => c.HubKey == hubDatajob.HubKey && c.IsValid)
	            .Where(a => a.IsValid && hubDatajob.Key == a.DatajobKey).LoadAsync();
            
            await LoadParametersDependencies(hubDatajob.Parameters, dbContext);

            if(includeDependencies)
            {
                if (hubDatajob.AuditConnectionKey != null)
                {
                    await AddConnections(new[] {hubDatajob.AuditConnectionKey.Value}, false, dbContext);
                }
                
                await AddDatalinks(hubDatajob.DexihDatalinkSteps.Where(c => c != null).Select(c => c.DatalinkKey.Value).ToArray(), dbContext);
            }
        }
	    
        public void LoadDatajobDependencies(DexihDatajob hubDatajob, DexihHub hub)
	    {
		    LoadParametersDependencies(hubDatajob.Parameters, hub);

			if (hubDatajob.AuditConnectionKey != null)
			{
				AddConnections(new[] {hubDatajob.AuditConnectionKey.Value}, false, hub);
			}

			AddDatalinks(hubDatajob.DexihDatalinkSteps.Where(c => c.DatalinkKey != null).Select(c => c.DatalinkKey.Value).ToArray(), hub);
	    }

        public async Task LoadColumnValidationDependencies(DexihColumnValidation columnValidation, DexihRepositoryContext dbContext)
        {
            if(columnValidation.LookupColumnKey != null)
            {
                var column = await dbContext.DexihTableColumns.SingleOrDefaultAsync(c => c.IsValid && c.Key == columnValidation.LookupColumnKey);
                await AddTables(new[] { column.TableKey.Value }, dbContext);
            }
        }
	    
        public void LoadColumnValidationDependencies(DexihColumnValidation columnValidation, DexihHub hub)
	    {
		    if(columnValidation.LookupColumnKey != null)
		    {
			    var tableColumn = hub.GetTableColumnFromKey(columnValidation.LookupColumnKey.Value);
			    AddTables(new[] { tableColumn.table.Key }, hub);
		    }
	    }
	    
        public async Task AddConnections(IEnumerable<long> connectionKeys, bool includeTables, DexihRepositoryContext dbContext)
        {
            foreach (var connectionKey in connectionKeys)
            {
                var connection = Hub.DexihConnections.SingleOrDefault(c => c.Key == connectionKey);
                if(connection == null)
                {
                    connection = await dbContext.DexihConnections.SingleOrDefaultAsync(c => c.HubKey == HubKey && c.Key == connectionKey && c.IsValid);
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
		    var connections = hub.DexihConnections.Where(c => connectionKeys.ToArray().Contains(c.Key));
		    foreach (var connection in connections)
		    {
			    var existingConnection = Hub.DexihConnections.SingleOrDefault(c => c.Key == connection.Key);
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
            var datalinks = await GetDatalinksAsync(datalinkKeys, dbContext);
            
            foreach (var datalink in datalinks)
            {
                await LoadDatalinkDependencies(datalink, true, dbContext);
                Hub.DexihDatalinks.Add(datalink);
            }
        }

	    public void AddDatalinks(IEnumerable<long> datalinkKeys, DexihHub hub)
	    {
		    var datalinks = hub.DexihDatalinks.Where(c => datalinkKeys.ToArray().Contains(c.Key));
		    foreach (var datalink in datalinks)
		    {
			    var existingDatalink = Hub.DexihDatalinks.SingleOrDefault(c => c.Key == datalink.Key);
			    if(existingDatalink == null)
			    {
				    Hub.DexihDatalinks.Add(datalink);
				    LoadDatalinkDependencies(datalink, hub);
			    }
		    }
	    }

	    public void AddDatalinkTests(IEnumerable<long> datalinkTestKeys, DexihHub hub)
	    {
		    var datalinkTests = hub.DexihDatalinkTests.Where(c => datalinkTestKeys.ToArray().Contains(c.Key));
		    foreach (var datalinkTest in datalinkTests)
		    {
			    var existingDatalinkTest = Hub.DexihDatalinkTests.SingleOrDefault(c => c.Key == datalinkTest.Key);
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
	            var table = Hub.DexihTables.SingleOrDefault(c => c.Key == tableKey);
	            if (table == null)
	            {
		            var hubTable = await dbContext.DexihTables.SingleOrDefaultAsync(c => c.HubKey == HubKey && c.Key == tableKey && c.IsValid);
		            if (hubTable == null)
		            {
			            throw new CacheManagerException($"Could not find the table with the key {tableKey}.");
		            }

		            await LoadTableColumns(hubTable, dbContext);

		            await AddConnections(new[] { hubTable.ConnectionKey }, false, dbContext);

	                
		            if(hubTable.FileFormatKey != null)
		            {
			            await AddFileFormats(new []{hubTable.FileFormatKey.Value}, dbContext);
		            }
		            Hub.DexihTables.Add(hubTable);
	            }
            }
        }
	    
	    public void AddTables(IEnumerable<long> tableKeys, DexihHub hub)
	    {
		    foreach (var tableKey in tableKeys)
		    {
			    var table = Hub.DexihTables.SingleOrDefault(c => c.Key == tableKey);
			    if (table == null)
			    {
				    var hubTable = hub.DexihTables.SingleOrDefault(c=>c.Key == tableKey);

				    if (hubTable == null)
				    {
					    throw new CacheManagerException($"Could not find the table with the key {tableKey}.");
				    }

				    LoadTableColumns(hubTable, hub);
				    
				    AddConnections(new[] { hubTable.ConnectionKey }, false, hub);
				    
				    if(hubTable.FileFormatKey != null)
				    {
					    AddFileFormats(new []{hubTable.FileFormatKey.Value}, hub);
				    }

				    Hub.DexihTables.Add(hubTable);
			    }
		    }
	    }

	    /// <summary>
	    /// Adds a table object to the cache along with any dependencies.  This is used where the table has not been saved to the repository.
	    /// Ensure the tableKey is unique.
	    /// </summary>
	    /// <param name="hubTable"></param>
	    /// <param name="hub"></param>
	    public void AddTable(DexihTable hubTable, DexihHub hub)
	    {
		    AddConnections(new[] { hubTable.ConnectionKey }, false, hub);
		    Hub.DexihTables.Add(hubTable);
				    
		    if(hubTable.FileFormatKey != null)
		    {
			    AddFileFormats(new []{hubTable.FileFormatKey.Value}, hub);
		    }
	    }

	    public void AddFileFormats(IEnumerable<long> fileFormatKeys, DexihHub hub)
	    {
		    foreach (var fileFormatKey in fileFormatKeys)
		    {
			    var fileFormat = Hub.DexihFileFormats.SingleOrDefault(c => c.Key == fileFormatKey);
			    if(fileFormat == null)
			    {
				    fileFormat = hub.DexihFileFormats.SingleOrDefault(c => c.Key == fileFormatKey && c.IsValid);
				    Hub.DexihFileFormats.Add(fileFormat);
			    }
		    }
	    }
	    
	    public async Task AddFileFormats(IEnumerable<long> fileFormatKeys, DexihRepositoryContext dbContext)
	    {
		    foreach (var fileFormatKey in fileFormatKeys)
		    {
			    var fileFormat = Hub.DexihFileFormats.SingleOrDefault(c => c.Key == fileFormatKey);
			    if(fileFormat == null)
			    {
				    fileFormat = await dbContext.DexihFileFormats.SingleOrDefaultAsync(c => c.HubKey == HubKey && c.Key == fileFormatKey && c.IsValid);
				    Hub.DexihFileFormats.Add(fileFormat);
			    }
		    }
	    }

	    /// <summary>
	    /// Adds a datalink to the cache, along with any dependencies.
	    /// </summary>
	    /// <param name="hubDatalink"></param>
	    /// <param name="hub"></param>
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
		    
		    LoadParametersDependencies(hubDatalink.Parameters, hub);
		    
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
                var existingJob = Hub.DexihDatajobs.SingleOrDefault(c=>c.Key == datajobKey);
                if(existingJob == null)
                {
                    var job = await dbContext.DexihDatajobs.SingleOrDefaultAsync(c => c.HubKey == HubKey && c.Key == datajobKey && c.IsValid);
                    Hub.DexihDatajobs.Add(job);
	                await LoadDatajobDependencies(job, true, dbContext);
                }
            }
        }
	    
	    public void AddDatajobs(IEnumerable<long> datajobKeys, DexihHub hub)
	    {
		    foreach (var datajobKey in datajobKeys)
		    {
			    var existingJob = Hub.DexihDatajobs.SingleOrDefault(c=>c.Key == datajobKey);
			    if(existingJob == null)
			    {
				    var job = hub.DexihDatajobs.SingleOrDefault(c => c.Key == datajobKey && c.IsValid);
				    Hub.DexihDatajobs.Add(job);
				    LoadDatajobDependencies(job, hub);
			    }

		    }
	    }

	    public async Task AddColumnValidations(IEnumerable<long> columnValidationKeys, DexihRepositoryContext dbContext)
	    {
		    foreach (var columnValidationKey in columnValidationKeys)
		    {
			    var columnValidation = Hub.DexihColumnValidations.SingleOrDefault(c => c.Key == columnValidationKey);
			    if(columnValidation == null)
			    {
				    columnValidation = await dbContext.DexihColumnValidations.SingleOrDefaultAsync(c => c.HubKey == HubKey && c.Key == columnValidationKey && c.IsValid);
				    Hub.DexihColumnValidations.Add(columnValidation);
				    await LoadColumnValidationDependencies(columnValidation, dbContext);
			    }
		    }
	    }
	    
	    public void AddColumnValidations(IEnumerable<long> columnValidationKeys, DexihHub hub)
	    {
		    foreach (var columnValidationKey in columnValidationKeys)
		    {
			    var columnValidation = Hub.DexihColumnValidations.SingleOrDefault(c => c.Key == columnValidationKey);
			    if(columnValidation == null)
			    {
				    columnValidation = hub.DexihColumnValidations.SingleOrDefault(c => c.Key == columnValidationKey && c.IsValid);
				    Hub.DexihColumnValidations.Add(columnValidation);
				    LoadColumnValidationDependencies(columnValidation, hub);
			    }
		    }
	    }
        
        public async Task AddCustomFunctions(IEnumerable<long> customFunctionKeys, DexihRepositoryContext dbContext)
        {
            foreach (var customFunctionKey in customFunctionKeys)
            {
                var customFunction = Hub.DexihCustomFunctions.SingleOrDefault(c => c.Key == customFunctionKey);
                if(customFunction == null)
                {
                    customFunction = await dbContext.DexihCustomFunctions.SingleOrDefaultAsync(c => c.HubKey == HubKey && c.Key == customFunctionKey && c.IsValid);
                    Hub.DexihCustomFunctions.Add(customFunction);
                }
            }
        }

	    public void AddCustomFunctions(IEnumerable<long> customFunctionKeys, DexihHub hub)
	    {
		    foreach (var customFunctionKey in customFunctionKeys)
		    {
			    var customFunction = Hub.DexihCustomFunctions.SingleOrDefault(c => c.Key == customFunctionKey);
			    if(customFunction == null)
			    {
				    customFunction = hub.DexihCustomFunctions.SingleOrDefault(c => c.Key == customFunctionKey && c.IsValid);
				    Hub.DexihCustomFunctions.Add(customFunction);
			    }
		    }
	    }

	    public void AddViews(IEnumerable<long> viewKeys, DexihHub hub)
	    {
		    foreach (var viewKey in viewKeys)
		    {
			    var view = Hub.DexihViews.SingleOrDefault(c => c.Key == viewKey);
			    if(view == null)
			    {
				    view = hub.DexihViews.SingleOrDefault(c => c.Key == viewKey && c.IsValid);
				    Hub.DexihViews.Add(view);
				    
				    LoadViewDependencies(view, hub);
			    }
		    }
	    }
	    
	    public async Task AddViews(IEnumerable<long> viewKeys, DexihRepositoryContext dbContext)
	    {
		    foreach (var viewKey in viewKeys)
		    {
			    var view = Hub.DexihViews.SingleOrDefault(c => c.Key == viewKey);
			    if(view == null)
			    {
				    view = await dbContext.DexihViews.SingleOrDefaultAsync(c => c.HubKey == HubKey && c.Key == viewKey && c.IsValid);
				    await LoadViewDependencies(view, dbContext);
				    Hub.DexihViews.Add(view);
			    }
		    }
	    }
	    
	    public void AddDashboards(IEnumerable<long> dashboardKeys, DexihHub hub)
	    {
		    foreach (var dashboardKey in dashboardKeys)
		    {
			    var dashboard = Hub.DexihDashboards.SingleOrDefault(c => c.Key == dashboardKey);
			    if(dashboard == null)
			    {
				    dashboard = hub.DexihDashboards.SingleOrDefault(c => c.Key == dashboardKey && c.IsValid);
				    Hub.DexihDashboards.Add(dashboard);

				    LoadDashboardDependencies(dashboard, hub);
			    }
		    }
	    }
	    
	    public async Task AddDashboards(IEnumerable<long> dashboardKeys, DexihRepositoryContext dbContext)
	    {
		    foreach (var dashboardKey in dashboardKeys)
		    {
			    var dashboard = Hub.DexihDashboards.SingleOrDefault(c => c.Key == dashboardKey);
			    if(dashboard == null)
			    {
				    dashboard = await dbContext.DexihDashboards.SingleOrDefaultAsync(c => c.HubKey == HubKey && c.Key == dashboardKey && c.IsValid);
				    Hub.DexihDashboards.Add(dashboard);
				    await LoadDashboardDependencies(dashboard, dbContext);
			    }
		    }
	    }
	    
	    public void AddListOfValues(IEnumerable<long> listOfValuesKeys, DexihHub hub)
	    {
		    foreach (var key in listOfValuesKeys)
		    {
			    var lov = Hub.DexihListOfValues.SingleOrDefault(c => c.Key == key);
			    if(lov == null)
			    {
				    lov = hub.DexihListOfValues.SingleOrDefault(c => c.Key == key && c.IsValid);
				    if (lov == null)
				    {
					    throw new CacheManagerException($"The list of value with the key {key} could not be found.");
				    }
				    Hub.DexihListOfValues.Add(lov);
				    
				    LoadListOfValuesDependencies(lov, hub);
			    }
		    }
	    }
	    
	    public async Task AddListOfValues(IEnumerable<long> listOfValuesKeys, DexihRepositoryContext dbContext)
	    {
		    foreach (var key in listOfValuesKeys)
		    {
			    var listOfValues = Hub.DexihListOfValues.SingleOrDefault(c => c.Key == key);
			    if(listOfValues == null)
			    {
				    listOfValues = await dbContext.DexihListOfValues.SingleOrDefaultAsync(c => c.HubKey == HubKey && c.Key == key && c.IsValid);
				    await LoadListOfValuesDependencies(listOfValues, dbContext);
				    Hub.DexihListOfValues.Add(listOfValues);
			    }
		    }
	    }
	    
	    public void AddApis(IEnumerable<long> apiKeys, DexihHub hub)
	    {
		    foreach (var apiKey in apiKeys)
		    {
			    var api = Hub.DexihApis.SingleOrDefault(c => c.Key == apiKey);
			    if(api == null)
			    {
				    api = hub.DexihApis.SingleOrDefault(c => c.Key == apiKey && c.IsValid);
				    Hub.DexihApis.Add(api);
				    
				    if(api.SourceTableKey != null && api.SourceTableKey > 0) AddTables(new [] {api.SourceTableKey.Value}, hub);
				    if(api.SourceDatalinkKey != null && api.SourceDatalinkKey > 0) AddDatalinks(new [] {api.SourceDatalinkKey.Value}, hub);
			    }
		    }
	    }
	    
	    public async Task AddApis(IEnumerable<long> apiKeys, DexihRepositoryContext dbContext)
	    {
		    foreach (var apiKey in apiKeys)
		    {
			    var api = Hub.DexihApis.SingleOrDefault(c => c.Key == apiKey);
			    if(api == null)
			    {
				    api = await dbContext.DexihApis.SingleOrDefaultAsync(c => c.HubKey == HubKey && c.Key == apiKey && c.IsValid);
				    Hub.DexihApis.Add(api);
				    
				    if(api.SourceTableKey != null) await AddTables(new [] {api.SourceTableKey.Value}, dbContext);
				    if(api.SourceDatalinkKey != null) await AddDatalinks(new [] {api.SourceDatalinkKey.Value}, dbContext);
			    }
		    }
	    }
	    
		public async Task<DexihDatalink> GetDatalink(long datalinkKey, DexihRepositoryContext dbContext)
        {
	        var datalinks = await GetDatalinksAsync(new[] {datalinkKey}, dbContext);

	        if (datalinks.Any())
	        {
		        return datalinks[0];
	        }

	        throw new RepositoryManagerException($"A datalink with the key {datalinkKey} could not be found.");
        }
		
		public async Task<DexihDatalink[]> GetDatalinksAsync(IEnumerable<long> datalinkKeys, DexihRepositoryContext dbContext)
		{
            try
            {
	            var datalinks = await dbContext.DexihDatalinks
		            .Where(c => c.IsValid && c.HubKey == HubKey && datalinkKeys.Contains(c.Key))
		            .ToArrayAsync();

	            var datalinkTableKeys = datalinks.Select(c => c.SourceDatalinkTableKey).ToList();

	            await dbContext.DexihDatalinkParameters
		            // .Include(c => c.ProfileRule)
		            .Where(c => c.IsValid && c.Datalink.IsValid && c.Datalink.HubKey == HubKey && datalinkKeys.Contains(c.DatalinkKey))
		            .LoadAsync();
	            
	            await dbContext.DexihDatalinkProfiles
		            // .Include(c => c.ProfileRule)
		            .Where(c => c.IsValid && c.Datalink.IsValid && c.Datalink.HubKey == HubKey && datalinkKeys.Contains(c.DatalinkKey))
		            .LoadAsync();

	            await dbContext.DexihDatalinkTargets
		            .Where(c => c.IsValid && c.HubKey == HubKey && datalinkKeys.Contains(c.DatalinkKey))
		            .ToHashSetAsync();
		            
	            var transforms = await dbContext.DexihDatalinkTransforms
		            .Where(c => c.IsValid && c.HubKey == HubKey && datalinkKeys.Contains(c.DatalinkKey))
		            .ToHashSetAsync();

	            var datalinkTransformKeys = transforms.Select(c => c.Key);

	            var transformItems = await dbContext.DexihDatalinkTransformItems
		            // .Include(c => c.StandardFunction)
		            .Where(c => c.IsValid && c.HubKey == HubKey && datalinkTransformKeys.Contains(c.DatalinkTransformKey))
		            .OrderBy(c => c.Dt.Datalink.HubKey).ThenBy(c => c.Dt.Key)
		            .ThenBy(c => c.Key)
		            .ToHashSetAsync();

                var transformItemKeys = transformItems.Select(c => c.Key);

	            var parameters = await dbContext.DexihFunctionParameters
		            .Where(c => c.IsValid && c.HubKey == HubKey && transformItemKeys.Contains(c.DatalinkTransformItemKey))
		            .OrderBy(c => c.DtItem.Dt.Datalink.HubKey).ThenBy(c => c.DtItem.Dt.Key).ThenBy(c => c.DtItem.Key).ThenBy(c => c.Position)
		            .ToHashSetAsync();

	            var parameterKeys = parameters.Select(c => c.Key);
	            var parameterArrays = await dbContext.DexihFunctionArrayParameters
		            .Where(c => c.IsValid && c.HubKey == HubKey && parameterKeys.Contains(c.FunctionParameterKey))
		            .OrderBy(c => c.FunctionParameter.DtItem.Dt.Datalink.HubKey).ThenBy(c => c.FunctionParameter.DtItem.Dt.Key).ThenBy(c => c.FunctionParameter.DtItem.Key).ThenBy(c => c.Position)
		            .ToHashSetAsync();

	            datalinkTableKeys.AddRange(transforms.Where(c => c.JoinDatalinkTableKey != null).Select(c => c.JoinDatalinkTableKey.Value));

	            if (datalinkTableKeys.Any())
	            {
		            await dbContext.DexihDatalinkTables
			            .Where(c => c.IsValid && c.HubKey == HubKey && datalinkTableKeys.Contains(c.Key))
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
			            .Where(c => c.IsValid && c.HubKey == HubKey && datalinkColumnKeys.Contains(c.Key))
			            .LoadAsync();
	            }

	            return datalinks;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Get datalinks with keys {string.Join(", ", datalinkKeys)} failed.  {ex.Message}", ex);
            }

        }

		public async Task<DexihDatajob> GetDatajob(long datajobKey, DexihRepositoryContext dbContext)
        {
	        var datajobs = await GetDatajobs(new[] {datajobKey}, dbContext);

	        if (datajobs.Any())
	        {
		        return datajobs[0];
	        }

	        throw new RepositoryManagerException($"A datajob with the key {datajobKey} could not be found.");
        }
		
		public async Task<DexihDatajob[]> GetDatajobs(IEnumerable<long> datajobKeys, DexihRepositoryContext dbContext)
		{
            try
            {
	            var datajobs = await dbContext.DexihDatajobs
		            .Where(c => c.IsValid && c.HubKey == HubKey && datajobKeys.Contains(c.Key))
		            .ToArrayAsync();

	            await dbContext.DexihDatajobParameters
		            .Where(c => c.IsValid && c.Datajob.IsValid && c.Datajob.HubKey == HubKey && datajobKeys.Contains(c.DatajobKey))
		            .LoadAsync();

	            await dbContext.DexihTriggers
		            .Where(c => c.IsValid && c.Datajob.IsValid && c.Datajob.HubKey == HubKey && datajobKeys.Contains(c.DatajobKey))
		            .LoadAsync();

	            var steps = await dbContext.DexihDatalinkStep
		            .Where(c => c.IsValid && c.Datajob.IsValid && c.Datajob.HubKey == HubKey && datajobKeys.Contains(c.DatajobKey))
		            .ToHashSetAsync();

	            var stepKeys = steps.Select(c => c.Key).ToArray();

	            await dbContext.DexihDatalinkStepParameters
		            .Where(c => c.IsValid && c.DatalinkStep.IsValid && c.DatalinkStep.HubKey == HubKey && stepKeys.Contains(c.DatalinkStepKey))
		            .LoadAsync();

	            await dbContext.DexihDatalinkDependencies
		            .Where(c => c.IsValid && c.DatalinkStep.IsValid && c.DatalinkStep.HubKey == HubKey && (stepKeys.Contains(c.DatalinkStepKey) || stepKeys.Contains((c.DependentDatalinkStepKey))))
		            .LoadAsync();

	            return datajobs;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Get datajobs with keys {string.Join(", ", datajobKeys)} failed.  {ex.Message}", ex);
            }

        }
		
		public async Task<DexihTable[]> GetTablesAsync(IEnumerable<long> keys, DexihRepositoryContext dbContext)
		{
			try
			{
				var tables = await dbContext.DexihTables
					.Where(c => c.IsValid && c.HubKey == HubKey && keys.Contains(c.Key))
					.ToArrayAsync();

				await dbContext.DexihTableColumns
					.Where(c => c.IsValid && c.HubKey == HubKey && keys.Contains(c.TableKey.Value))
					.LoadAsync();

				return tables;
			}
			catch (Exception ex)
			{
				throw new RepositoryManagerException($"Get tables with keys {string.Join(", ", keys)} failed.  {ex.Message}", ex);
			}
		}
		
		public async Task<DexihView[]> GetViewsAsync(IEnumerable<long> keys, DexihRepositoryContext dbContext)
		{
			try
			{
				var views = await dbContext.DexihViews
					.Where(c => c.IsValid && c.HubKey == HubKey && keys.Contains(c.Key))
					.ToArrayAsync();

				await dbContext.DexihViewParameters
					.Where(c => c.IsValid && c.HubKey == HubKey && keys.Contains(c.ViewKey))
					.LoadAsync();

				return views;
			}
			catch (Exception ex)
			{
				throw new RepositoryManagerException($"Get tables with keys {string.Join(", ", keys)} failed.  {ex.Message}", ex);
			}
		}
		
		public async Task<DexihDashboard[]> GetDashboardsAsync(IEnumerable<long> keys, DexihRepositoryContext dbContext)
		{
			try
			{
				var dashboards = await dbContext.DexihDashboards
					.Where(c => c.IsValid && c.HubKey == HubKey && keys.Contains(c.Key))
					.ToArrayAsync();

				await dbContext.DexihDashboardItems
					.Where(c => c.IsValid && c.HubKey == HubKey && keys.Contains(c.DashboardKey))
					.LoadAsync();

				await dbContext.DexihDashboardParameters
					.Where(c => c.IsValid && c.HubKey == HubKey && keys.Contains(c.DashboardKey))
					.LoadAsync();

				return dashboards;
			}
			catch (Exception ex)
			{
				throw new RepositoryManagerException($"Get tables with keys {string.Join(", ", keys)} failed.  {ex.Message}", ex);
			}
		}
		
		public async Task<DexihApi[]> GetApisAsync(IEnumerable<long> keys, DexihRepositoryContext dbContext)
		{
			try
			{
				var apis = await dbContext.DexihApis
					.Where(c => c.IsValid && c.HubKey == HubKey && keys.Contains(c.Key))
					.ToArrayAsync();

				await dbContext.DexihApiParameters
					.Where(c => c.IsValid && c.HubKey == HubKey && keys.Contains(c.ApiKey))
					.LoadAsync();

				return apis;
			}
			catch (Exception ex)
			{
				throw new RepositoryManagerException($"Get tables with keys {string.Join(", ", keys)} failed.  {ex.Message}", ex);
			}
		}
		
	    public async Task<DexihDatalinkTest> GetDatalinkTest(long datalinkTestKey, DexihRepositoryContext dbContext)
	    {
		    var datalinkTest = await dbContext.DexihDatalinkTests.Include(c => c.DexihDatalinkTestSteps)
			    .ThenInclude(d => d.DexihDatalinkTestTables).SingleOrDefaultAsync(c=>c.Key == datalinkTestKey);

		    return datalinkTest;
	    }
    }
}

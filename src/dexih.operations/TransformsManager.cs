using dexih.functions;
using dexih.repository;
using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Dexih.Utils.CopyProperties;
using dexih.functions.Query;
using dexih.transforms.Transforms;
using Microsoft.Extensions.Logging;
using static dexih.repository.DexihDatalinkTable;

namespace dexih.operations
{
    public class TransformsManager
    {
        private readonly TransformSettings _transformSettings;
        private readonly ILogger _logger;

        public TransformsManager(TransformSettings transformSettings)
        {
            _transformSettings = transformSettings;
        }

        public TransformsManager(TransformSettings transformSettings, ILogger logger)
        {
            _transformSettings = transformSettings;
            _logger = logger;
        }

        /// <summary>
        /// Converts a table to DexihTalble.  If an originalTable is included, TableKeys and ColumnKeys will be preserved where possible.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="originalTable"></param>
        /// <returns></returns>
        public DexihTable GetDexihTable(Table table, DexihTable originalTable = null)
        {
            if (table == null)
                return null;

            var dbTable = originalTable ?? new DexihTable();

            foreach(var dbColumn in dbTable.DexihTableColumns)
            {
                dbColumn.IsValid = false;
            }

            table.CopyProperties(dbTable, true);
            dbTable.IsValid = true;

			if (table is FlatFile flatFile)
			{
			    dbTable.FileRootPath = flatFile.FileRootPath;
				dbTable.FileIncomingPath = flatFile.FileIncomingPath;
				dbTable.FileProcessedPath = flatFile.FileProcessedPath;
				dbTable.FileRejectedPath = flatFile.FileRejectedPath;
				dbTable.FileMatchPattern = flatFile.FileMatchPattern;
				dbTable.UseCustomFilePaths = flatFile.UseCustomFilePaths;
				dbTable.FileSample = flatFile.FileSample;
			}

			if(table is WebService restFunction)
			{
			    dbTable.RestfulUri = restFunction.RestfulUri;
				dbTable.RowPath = restFunction.RowPath;
			}


            if (originalTable != null)
            {
                var position = 1;
                foreach (var column in table.Columns)
                {
                    var dbColumn = originalTable.DexihTableColumns.SingleOrDefault(c => c.Name == column.Name);
                    if (dbColumn == null)
                    {
                        dbColumn = new DexihTableColumn {ColumnKey = 0};
                        originalTable.DexihTableColumns.Add(dbColumn);
                    }

                    column.CopyProperties(dbColumn);
                    dbColumn.Position = position;
                    dbColumn.IsValid = true;
                    position++;
                }

                //remove columns that do not exist in the source anymore
                foreach (var dbColumn in originalTable.DexihTableColumns.ToList())
                {
                    if (table.Columns.All(c => c.Name != dbColumn.Name))
                    {
                        originalTable.DexihTableColumns.Remove(dbColumn);
                    }
                }
            }

            return dbTable;
        }
        



        /// Searches all the tables in a datalink for a particular columnKey
        public DexihDatalinkColumn GetDatalinkColumn(DexihHub hub, DexihDatalink datalink, long? datalinkColumnKey)
        {
            if(datalinkColumnKey == null || datalink == null) 
                return null;

            DexihDatalinkColumn column = null;

            column = datalink.SourceDatalinkTable.DexihDatalinkColumns.SingleOrDefault(c => c.DatalinkColumnKey == (long)datalinkColumnKey);
            if(column != null)
                return column;

            foreach(var datalinkTransform in datalink.DexihDatalinkTransforms)
            {
                foreach(var item in datalinkTransform.DexihDatalinkTransformItems)
                {
                    if(item.SourceDatalinkColumnKey == datalinkColumnKey)
                    {
                        return item.SourceDatalinkColumn;
                    }
                    if(item.TargetDatalinkColumnKey == datalinkColumnKey)
                    {
                        return item.TargetDatalinkColumn;
                    }

                    foreach(var param in item.DexihFunctionParameters)
                    {
                        if (param.DatalinkColumnKey == datalinkColumnKey)
                        {
                            return param.DatalinkColumn;
                        }
                    }
                }

				if (datalinkTransform.JoinDatalinkTableKey != null)
				{
                    column = datalinkTransform.JoinDatalinkTable.DexihDatalinkColumns.SingleOrDefault(c => c.DatalinkColumnKey == (long)datalinkColumnKey);
					if (column != null)
						return column;
				}
            }

            return null;
        }





        public TransformFunction GetProfileFunction(string functionAssemblyName, string functionClassName, string functionMethodName, string columnName, bool detailedResults)
        {
            try
            {
                Type type;
                if (string.IsNullOrEmpty(functionAssemblyName))
                {
                    type = Type.GetType(functionClassName);
                }
                else
                {

                    var assemblyName = new AssemblyName(functionAssemblyName).Name;
                    var folderPath = Path.GetDirectoryName(this.GetType().GetTypeInfo().Assembly.Location);
                    var assemblyPath = Path.Combine(folderPath, assemblyName + ".dll");
                    if (!File.Exists(assemblyPath))
                    {
                        throw new TransformManagerException("The profile function could not be started due to a missing assembly.  The assembly name is: " + functionAssemblyName + ", and class: " + functionClassName+ ", and expected in directory: " + this.GetType().GetTypeInfo().Assembly.Location + ".  Has this been installed?");
                    }

                    var loader = new AssemblyLoader(folderPath);
                    var assembly = loader.LoadFromAssemblyName(new AssemblyName(assemblyName));
                    type = assembly.GetType(functionClassName);
                }

                var profileObject = Activator.CreateInstance(type);

                var property = profileObject.GetType().GetProperty("DetailedResults");
                if(property != null)
                    property.SetValue(profileObject, detailedResults);

                var profileFunction = new TransformFunction(profileObject, functionMethodName, new TableColumn[] { new TableColumn(columnName) }, null, null);
                return profileFunction;
            }
            catch (Exception ex)
            {
                throw new TransformManagerException($"Get profile function failed.  {ex.Message}.", ex);
            }
        }

        public TransformFunction GetValidationFunction(DexihColumnValidation columnValidation, string columnName)
        {
            var inputs = new string[] { columnName };
            var outputs = new string[] { columnName };

            var validationFunction = new TransformFunction(this, this.GetType().GetMethod("Run"), new TableColumn[] { new TableColumn(columnName) }, new TableColumn(columnName), new TableColumn[] { new TableColumn(columnName), new TableColumn("RejectReason") })
            {
                InvalidAction = columnValidation.InvalidAction
            };
            return validationFunction;
        }

   

  

    

		public (Transform sourceTransform, Table sourceTable) GetSourceTransform(DexihHub hub, DexihDatalinkTable datalinkTable)
		{
            try
            {
                switch (datalinkTable.SourceType)
                {
                    case ESourceType.Datalink:
                        var datalink = hub.DexihDatalinks.SingleOrDefault(c => c.DatalinkKey == datalinkTable.SourceDatalinkKey);
                        if (datalink == null)
                        {
                            throw new TransformManagerException($"The source datalink with the key {datalinkTable.SourceDatalinkKey.Value} was not found");
                        }
                        var result = CreateRunPlan(hub, datalink, null, null, false);
                        result.sourceTransform.ReferenceTableAlias = datalinkTable.DatalinkTableKey.ToString();
                        return result;

                    case ESourceType.Table:
                        if (datalinkTable.SourceTableKey == null)
                        {
                            throw new TransformManagerException($"The source table key was null.");
                        }
                        var sourceDbTable = hub.GetTableFromKey(datalinkTable.SourceTableKey.Value);
                        if (sourceDbTable == null)
                        {
                            throw new TransformManagerException($"The source table with the key {datalinkTable.SourceTableKey.Value} could not be found.");
                        }
                        
                        var sourceTable = datalinkTable.GetTable(sourceDbTable);
                        
                        if (sourceDbTable.IsInternal)
                        {
                            var rowCreator = new ReaderRowCreator();
                            rowCreator.InitializeRowCreator(0, 0, 1);
                            rowCreator.ReferenceTableAlias = datalinkTable.DatalinkTableKey.ToString();
                            return (rowCreator, sourceTable);
                        }
                        else
                        {
                            var sourceDbConnection = hub.DexihConnections.SingleOrDefault(c => c.ConnectionKey == sourceDbTable.ConnectionKey && c.IsValid);
                            var sourceConnection = sourceDbConnection.GetConnection(_transformSettings);
                            var transform = sourceConnection.GetTransformReader(sourceTable);
                            transform.ReferenceTableAlias = datalinkTable.DatalinkTableKey.ToString();
                            return (transform, sourceTable);
                        }

                    case ESourceType.Rows:
                        var rowCreator2 = new ReaderRowCreator();
                        rowCreator2.InitializeRowCreator(datalinkTable.RowsStartAt??1, datalinkTable.RowsEndAt??1, datalinkTable.RowsIncrement??1);
                        rowCreator2.ReferenceTableAlias = datalinkTable.DatalinkTableKey.ToString();
                        var table = rowCreator2.GetTable();
                        return (rowCreator2, table);
                    case ESourceType.Function:
                        var functionTable = datalinkTable.GetTable();
                        var data = new object[functionTable.Columns.Count];
                        for(var i = 0; i< functionTable.Columns.Count; i++)
                        {
                            data[i] = functionTable.Columns[i].DefaultValue;
                        }
                        functionTable.Data.Add(data);
                        var defaultRow = new ReaderDynamic(functionTable);
                        defaultRow.Reset();
                        return (defaultRow, functionTable);

                    default:
                        throw new TransformManagerException($"Error getting the source transform.");
                }
            }
            catch (Exception ex)
            {
                throw new TransformManagerException($"Get source transform failed.  {ex.Message}", ex);
            }
        }

        public (Transform sourceTransform, Table sourceTable) CreateRunPlan(DexihHub hub, DexihDatalink datalink, long? maxDatalinkTransformKey, object maxIncrementalValue, bool truncateTargetTable = false, SelectQuery selectQuery = null) //Last datatransform key is used to preview the output of a specific transform in the series.
        {
            try
            {
                _logger?.LogTrace($"CreateRunPlan {datalink.Name} started.");

                var timer = Stopwatch.StartNew();
                
				var primaryTransformResult = GetSourceTransform(hub, datalink.SourceDatalinkTable);
				var primaryTransform = primaryTransformResult.sourceTransform;
				var sourceTable = primaryTransformResult.sourceTable;
				foreach(var column in primaryTransform.CacheTable.Columns)
				{
					column.ReferenceTable = datalink.SourceDatalinkTableKey.ToString();
				}

                //add a filter for the incremental column (if there is one)
                var incrementalCol = sourceTable?.GetIncrementalUpdateColumn();
                var updateStrategy = datalink.UpdateStrategy;

                if (truncateTargetTable == false && 
                    updateStrategy != TransformDelta.EUpdateStrategy.Reload && 
                    incrementalCol != null && 
                    (updateStrategy != TransformDelta.EUpdateStrategy.AppendUpdateDelete || updateStrategy != TransformDelta.EUpdateStrategy.AppendUpdateDeletePreserve) && 
                    maxIncrementalValue != null && 
                    maxIncrementalValue.ToString() != "")
                {
                    var filterPair = new FilterPair()
                    {
                        Column1 = incrementalCol,
                        FilterValue = maxIncrementalValue,
                        Compare = Filter.ECompare.GreaterThan
                    };

                    var filterTransform = new TransformFilter(primaryTransform, null, new List<FilterPair> { filterPair} );
                    filterTransform.SetInTransform(primaryTransform);
                    primaryTransform = filterTransform;
                }

                _logger?.LogTrace($"CreateRunPlan {datalink.Name}.  Added incremental filter.  Elapsed: {timer.Elapsed}");

                //loop through the transforms to create the chain.
                foreach (var datalinkTransform in datalink.DexihDatalinkTransforms.OrderBy(c => c.Position))
                {
                    var transform = datalinkTransform.GetTransform(hub, _logger);

                    _logger?.LogTrace($"CreateRunPlan {datalink.Name}, adding transform {datalinkTransform.Name}.  Elapsed: {timer.Elapsed}");

                    //if this is an empty transform, then ignore it.
                    if (datalinkTransform.DexihDatalinkTransformItems.Count == 0)
                    {
                        if (datalinkTransform.TransformType == TransformAttribute.ETransformType.Filter || (datalinkTransform.TransformType == TransformAttribute.ETransformType.Mapping && datalinkTransform.PassThroughColumns))
                        {
                            if (datalinkTransform.DatalinkTransformKey == maxDatalinkTransformKey)
                                break;

                            continue;
                        }
                    }

                    //if this is a validation transform. add the column validations also.
                    if (datalinkTransform.TransformType == TransformAttribute.ETransformType.Validation)
                    {
						if (datalink.VirtualTargetTable && datalink.TargetTableKey != null)
						{
							var targetTable = hub.GetTableFromKey(datalink.TargetTableKey.Value);
							if(targetTable == null)
							{
								throw new TransformManagerException($"The target table with the key {datalink.TargetTableKey} was not found.");
							}

							foreach (var column in datalink.TargetTable.DexihTableColumns.Where(c => c.ColumnValidationKey != null))
							{
								var columnValidation = hub.DexihColumnValidations.SingleOrDefault(c => c.ColumnValidationKey == column.ColumnValidationKey);
								transform.Functions.Add(GetValidationFunction(columnValidation, column.Name));
							}
						}

                        _logger?.LogTrace($"CreateRunPlan {datalink.Name}, adding validation.  Elapsed: {timer.Elapsed}");

                    }

					//if contains a jointable, then add it in.
					Transform referenceTransform = null;
					if(datalinkTransform.JoinDatalinkTable != null) 
					{
						var joinTransformResult = GetSourceTransform(hub, datalinkTransform.JoinDatalinkTable);
						referenceTransform = joinTransformResult.sourceTransform;
					}
                    
                    _logger?.LogTrace($"CreateRunPlan {datalink.Name}, adding transform {datalinkTransform.Name}, added joins.  Elapsed: {timer.Elapsed}");

                    transform.SetInTransform(primaryTransform, referenceTransform);

                    _logger?.LogTrace($"CreateRunPlan {datalink.Name}, adding transform {datalinkTransform.Name}.  set Inbound Transforms  Elapsed: {timer.Elapsed}");

                    primaryTransform = transform;

                    if (datalinkTransform.DatalinkTransformKey == maxDatalinkTransformKey)
                        break;
                }

                //if the maxDatalinkTransformKey is null (i.e. we are not doing a preview), and there are profiles add a profile transform.
                if (maxDatalinkTransformKey == null && datalink.DexihDatalinkProfiles != null && datalink.DexihDatalinkProfiles.Count > 0 && datalink.TargetTableKey != null)
                {
					var targetTable = hub.GetTableFromKey((long)datalink.TargetTableKey);

                    var profileRules = new List<TransformFunction>();
                    foreach (var profile in datalink.DexihDatalinkProfiles)
                    {
                        foreach (var column in targetTable.DexihTableColumns.Where(c => c.IsSourceColumn))
                        {
                            var profileFunction = GetProfileFunction(profile.FunctionAssemblyName, profile.FunctionClassName, profile.FunctionMethodName, column.Name, profile.DetailedResults);
                            profileRules.Add(profileFunction);
                        }
                    }
                    var transform = new TransformProfile(primaryTransform, profileRules);

                    primaryTransform = transform;

                    _logger?.LogTrace($"CreateRunPlan {datalink.Name}, adding profiling.  Elapsed: {timer.Elapsed}");
                }

                if(selectQuery != null)
                {
                    var transform = new TransformQuery(primaryTransform, selectQuery);
                    primaryTransform = transform;
                }

                _logger?.LogTrace($"CreateRunPlan {datalink.Name}, completed.  Elapsed: {timer.Elapsed}");

				return (primaryTransform, sourceTable);
            }
            catch (Exception ex)
            {
                throw new TransformManagerException($"Create run plan failed.  {ex.Message}", ex);
            }
        }

        

        /// <summary>
        /// Gets a preview of the table.
        /// </summary>
        /// <param name="dbTable"></param>
        /// <param name="hub"></param>
        /// <param name="query"></param>
        /// <param name="rejectedTable"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<Table> GetPreview(DexihTable dbTable, DexihHub hub, SelectQuery query, bool rejectedTable, CancellationToken cancellationToken)
        {
            try
            {
                var dbConnection = hub.DexihConnections.SingleOrDefault(c => c.ConnectionKey == dbTable.ConnectionKey && c.IsValid);
                if (dbConnection == null)
                {
                    throw new TransformManagerException($"The connection with the key {dbTable.ConnectionKey} was not found.");
                }
                

                var connection = dbConnection.GetConnection(_transformSettings);
                var table = rejectedTable ? dbTable.GetRejectedTable(connection, _transformSettings) : dbTable.GetTable(connection, _transformSettings);

                var previewResult = await connection.GetPreview(table, query, cancellationToken);

                return previewResult;
            }
            catch (Exception ex)
            {
                throw new TransformManagerException($"Get input transform failed.  {ex.Message}", ex);
            }
        }
        

    }
}

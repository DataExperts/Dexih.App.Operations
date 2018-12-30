using dexih.functions;
using dexih.repository;
using dexih.transforms;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using dexih.functions.Parameter;
using Dexih.Utils.CopyProperties;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;
using Microsoft.Extensions.Logging;

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
        /// Converts a table to DexihTable.  If an originalTable is included, TableKeys and ColumnKeys will be preserved where possible.
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


        /// <summary>
        /// Gets a reference to the specified profile function.
        /// </summary>
        /// <param name="functionAssemblyName"></param>
        /// <param name="functionClassName"></param>
        /// <param name="functionMethodName"></param>
        /// <param name="columnName"></param>
        /// <param name="globalVariables"></param>
        /// <returns></returns>
        /// <exception cref="TransformManagerException"></exception>
        public Mapping GetProfileFunction(string functionAssemblyName, string functionClassName, string functionMethodName, string columnName, bool detailedResults, GlobalVariables globalVariables)
        {
            try
            {
                var functionMethod =
                    Functions.GetFunctionMethod(functionClassName, functionMethodName, functionAssemblyName);

                var profileObject = Activator.CreateInstance(functionMethod.type);
                
                var property = profileObject.GetType().GetProperty("DetailedResults");
                if (property != null)
                {
                    property.SetValue(profileObject, detailedResults);
                }

                var parameters = new Parameters()
                {
                    Inputs = new Parameter[] {new ParameterColumn("value", new TableColumn(columnName))}
                };
                var profileFunction = new TransformFunction(profileObject, functionMethodName, null, parameters, globalVariables);
                var mapFunction = new MapFunction(profileFunction, parameters, MapFunction.EFunctionCaching.NoCache);
                return mapFunction;
            }
            catch (Exception ex)
            {
                throw new TransformManagerException($"Get profile function failed.  {ex.Message}.", ex);
            }
        }


		public (Transform sourceTransform, Table sourceTable) GetSourceTransform(DexihHub hub, DexihDatalinkTable datalinkTable, InputColumn[] inputColumns, GlobalVariables globalVariables, bool previewMode)
		{
            try
            {
                (Transform sourceTransform, Table sourceTable) returnValue;
                
                switch (datalinkTable.SourceType)
                {
                    case ESourceType.Datalink:
                        var datalink = hub.DexihDatalinks.SingleOrDefault(c => c.DatalinkKey == datalinkTable.SourceDatalinkKey);
                        if (datalink == null)
                        {
                            throw new TransformManagerException($"The source datalink with the key {datalinkTable.SourceDatalinkKey} was not found");
                        }
                        returnValue = CreateRunPlan(hub, datalink, null, globalVariables, null, false, previewMode: previewMode);
                        returnValue.sourceTransform.ReferenceTableAlias = datalinkTable.DatalinkTableKey.ToString();
                        break;
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
                        
//                        if (sourceDbTable.IsInternal)
//                        {
//                            var sourceTable = datalinkTable.GetTable(null, inputColumns);
//                            var rowCreator = new ReaderRowCreator();
//                            rowCreator.InitializeRowCreator(0, 0, 1);
//                            rowCreator.ReferenceTableAlias = datalinkTable.DatalinkTableKey.ToString();
//                            returnValue = (rowCreator, sourceTable);
//                        }
//                        else
//                        {
                            var sourceDbConnection = hub.DexihConnections.SingleOrDefault(c => c.ConnectionKey == sourceDbTable.ConnectionKey && c.IsValid);

                            if (sourceDbConnection == null)
                            {
                                throw new TransformException($"The connection with key {sourceDbTable.ConnectionKey} could not be found.");
                            }

                            var sourceConnection = sourceDbConnection.GetConnection(_transformSettings);
                            var sourceTable = sourceDbTable.GetTable(sourceConnection, inputColumns, _transformSettings);
                            var transform = sourceConnection.GetTransformReader(sourceTable, previewMode);
                            transform.ReferenceTableAlias = datalinkTable.DatalinkTableKey.ToString();
                            returnValue =  (transform, sourceTable);
//                        }

                        break;
                    case ESourceType.Rows:
                        var rowCreator2 = new ReaderRowCreator();
                        rowCreator2.InitializeRowCreator(datalinkTable.RowsStartAt??1, datalinkTable.RowsEndAt??1, datalinkTable.RowsIncrement??1);
                        rowCreator2.ReferenceTableAlias = datalinkTable.DatalinkTableKey.ToString();
                        var table = rowCreator2.GetTable();
                        returnValue =  (rowCreator2, table);
                        break;
                    case ESourceType.Function:
                        var functionTable = datalinkTable.GetTable(null, inputColumns);
                        var data = new object[functionTable.Columns.Count];
                        for(var i = 0; i< functionTable.Columns.Count; i++)
                        {
                            data[i] = functionTable.Columns[i].DefaultValue;
                        }
                        functionTable.Data.Add(data);
                        var defaultRow = new ReaderDynamic(functionTable);
                        defaultRow.Reset();
                        returnValue = (defaultRow, functionTable);
                        break;

                    default:
                        throw new TransformManagerException($"Error getting the source transform.");
                    
                }
                
                // compare the table in the transform to the source datalink columns.  If any are misisng, add a mapping 
                // transform to include them.
                var transformColumns = returnValue.sourceTransform.CacheTable.Columns;
                var datalinkColumns = datalinkTable.DexihDatalinkColumns;

                // var mappings = new List<ColumnPair>();
                var mappings = new Mappings();
                
                foreach (var column in datalinkColumns)
                {
                    var transformColumn = transformColumns.SingleOrDefault(c => c.Name == column.Name);
                    if (transformColumn == null)
                    {
                        var newColumn = column.GetTableColumn(inputColumns);
                        mappings.Add(new MapColumn(newColumn)); 
                    }
                }

                if (mappings.Count > 0)
                {
                    var transforMapping = new TransformMapping(returnValue.sourceTransform, mappings);
                    returnValue.sourceTransform = transforMapping;
                }

                return returnValue;
            }
            catch (Exception ex)
            {
                throw new TransformManagerException($"Get source transform failed.  {ex.Message}", ex);
            }
        }

        private void MergeInputColumn(TableColumn column, InputColumn[] inputColumns)
        {
            
        }

        public (Transform sourceTransform, Table sourceTable) CreateRunPlan(DexihHub hub, DexihDatalink datalink, InputColumn[] inputColumns, GlobalVariables globalVariables, long? maxDatalinkTransformKey, object maxIncrementalValue, bool truncateTargetTable = false, SelectQuery selectQuery = null, bool previewMode = false) //Last datatransform key is used to preview the output of a specific transform in the series.
        {
            try
            {
                _logger?.LogTrace($"CreateRunPlan {datalink.Name} started.");

                var timer = Stopwatch.StartNew();
                
				var primaryTransformResult = GetSourceTransform(hub, datalink.SourceDatalinkTable, inputColumns, globalVariables, previewMode);
				var primaryTransform = primaryTransformResult.sourceTransform;
				var sourceTable = primaryTransformResult.sourceTable;
				foreach(var column in primaryTransform.CacheTable.Columns)
				{
					column.ReferenceTable = datalink.SourceDatalinkTableKey.ToString();
				}

                //add a filter for the incremental column (if there is one)
                var incrementalCol = sourceTable?.GetIncrementalUpdateColumn();
                var updateStrategy = datalink.UpdateStrategy;

                if (maxDatalinkTransformKey == null && 
                    truncateTargetTable == false && 
                    updateStrategy != TransformDelta.EUpdateStrategy.Reload && 
                    incrementalCol != null && 
                    (updateStrategy != TransformDelta.EUpdateStrategy.AppendUpdateDelete || updateStrategy != TransformDelta.EUpdateStrategy.AppendUpdateDeletePreserve) && 
                    maxIncrementalValue != null && 
                    maxIncrementalValue.ToString() != "")
                {

                    var mappings = new Mappings()
                    {
                        new MapFilter(incrementalCol, maxIncrementalValue, Filter.ECompare.GreaterThan)
                    };

                    var filterTransform = new TransformFilter(primaryTransform, mappings);
                    filterTransform.SetInTransform(primaryTransform);
                    primaryTransform = filterTransform;
                }

                _logger?.LogTrace($"CreateRunPlan {datalink.Name}.  Added incremental filter.  Elapsed: {timer.Elapsed}");

                
                //loop through the transforms to create the chain.
                foreach (var datalinkTransform in datalink.DexihDatalinkTransforms.OrderBy(c => c.Position))
                {
                    var transform = datalinkTransform.GetTransform(hub, globalVariables, _transformSettings, _logger);

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
						if (!datalink.VirtualTargetTable && datalink.TargetTableKey != null)
						{
							var targetTable = hub.GetTableFromKey(datalink.TargetTableKey.Value);
							if(targetTable == null)
							{
								throw new TransformManagerException($"The target table with the key {datalink.TargetTableKey} was not found.");
							}

							foreach (var column in targetTable.DexihTableColumns.Where(c => c.ColumnValidationKey != null))
							{
								var columnValidation = hub.DexihColumnValidations.Single(c => c.ColumnValidationKey == column.ColumnValidationKey);
							    var validation =
							        new ColumnValidationRun(_transformSettings, columnValidation, hub)
							        {
							            DefaultValue = column.DefaultValue
							        };
							    var function = validation.GetValidationMapping(column.Name);
                                transform.Mappings.Add(function);
							}
						}

                        _logger?.LogTrace($"CreateRunPlan {datalink.Name}, adding validation.  Elapsed: {timer.Elapsed}");

                    }

					//if contains a jointable, then add it in.
					Transform referenceTransform = null;
					if(datalinkTransform.JoinDatalinkTable != null) 
					{
						var joinTransformResult = GetSourceTransform(hub, datalinkTransform.JoinDatalinkTable, null, globalVariables, previewMode);
						referenceTransform = joinTransformResult.sourceTransform;
					}
					
					// if transform uses a different node level add a node mapping
                    if (datalinkTransform.NodeDatalinkColumn != null)
                    {
                        var table = primaryTransform.CacheTable;

                        // create a path from the parent to the child column.
                        var columnPath = new Queue<DexihDatalinkColumn>();
                        var currentColumn = datalinkTransform.NodeDatalinkColumn;
                        while (currentColumn != null)
                        {
                            columnPath.Enqueue(currentColumn);
                            currentColumn = currentColumn.ParentColumn;
                        }

                        currentColumn = columnPath.Dequeue();
                        var nodeColumn = table.Columns[currentColumn.Name, currentColumn.ColumnGroup];
                        
                        while(columnPath.Any())
                        {
                            currentColumn = columnPath.Dequeue();
                            nodeColumn = nodeColumn.ChildColumns[currentColumn.Name, currentColumn.ColumnGroup];
                        }
                        
                        var mapNode = new MapNode(nodeColumn, table);
                        var nodeTransform = mapNode.Transform;
                        var nodeMapping = new TransformMapping(nodeTransform, transform.Mappings);
                        mapNode.OutputTransform = nodeMapping;
                        
                        var mappings = new Mappings {mapNode};
                        transform.Mappings = mappings;
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

                    var profileRules = new Mappings();
                    foreach (var profile in datalink.DexihDatalinkProfiles)
                    {
                        foreach (var column in targetTable.DexihTableColumns.Where(c => c.IsSourceColumn))
                        {
                            var profileFunction = GetProfileFunction(profile.FunctionAssemblyName, profile.FunctionClassName, profile.FunctionMethodName, column.Name, profile.DetailedResults, globalVariables);
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

        

//        /// <summary>
//        /// Gets a preview of the table.
//        /// </summary>
//        /// <param name="dbTable"></param>
//        /// <param name="hub"></param>
//        /// <param name="query"></param>
//        /// <param name="rejectedTable"></param>
//        /// <param name="cancellationToken"></param>
//        /// <returns></returns>
//        public async Task<Table> GetPreview(DexihTable dbTable, DexihHub hub, SelectQuery query, bool rejectedTable, CancellationToken cancellationToken)
//        {
//            try
//            {
//                var dbConnection = hub.DexihConnections.SingleOrDefault(c => c.ConnectionKey == dbTable.ConnectionKey && c.IsValid);
//                if (dbConnection == null)
//                {
//                    throw new TransformManagerException($"The connection with the key {dbTable.ConnectionKey} was not found.");
//                }
//                
//
//                var connection = dbConnection.GetConnection(_transformSettings);
//                var table = rejectedTable ? dbTable.GetRejectedTable(connection, _transformSettings) : dbTable.GetTable(connection, _transformSettings);
//
//                var previewResult = await connection.GetPreview(table, query, cancellationToken);
//
//                return previewResult;
//            }
//            catch (Exception ex)
//            {
//                throw new TransformManagerException($"Get input transform failed.  {ex.Message}", ex);
//            }
//        }
        

    }
}

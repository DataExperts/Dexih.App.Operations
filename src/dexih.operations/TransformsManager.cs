using dexih.functions;
using dexih.repository;
using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using dexih.functions.Parameter;
using Dexih.Utils.CopyProperties;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;
using Dexih.Utils.DataType;
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
                        dbColumn = new DexihTableColumn {Key = 0};
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
            else
            {
                var position = 1;
                foreach (var column in table.Columns)
                {
                    var dbColumn = new DexihTableColumn {Key = 0};
                    dbTable.DexihTableColumns.Add(dbColumn);

                    column.CopyProperties(dbColumn);
                    dbColumn.Position = position;
                    dbColumn.IsValid = true;
                    position++;
                }
            }

            return dbTable;
        }
        

        /// Searches all the tables in a datalink for a particular columnKey
        public DexihDatalinkColumn GetDatalinkColumn(DexihHub hub, DexihDatalink hubDatalink, long? datalinkColumnKey)
        {
            if(datalinkColumnKey == null || hubDatalink == null) 
                return null;

            DexihDatalinkColumn column = null;

            column = hubDatalink.SourceDatalinkTable.DexihDatalinkColumns.SingleOrDefault(c => c.Key == (long)datalinkColumnKey);
            if(column != null)
                return column;

            foreach(var datalinkTransform in hubDatalink.DexihDatalinkTransforms)
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
                    column = datalinkTransform.JoinDatalinkTable.DexihDatalinkColumns.SingleOrDefault(c => c.Key == (long)datalinkColumnKey);
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
                    Inputs = new Parameter[] {new ParameterColumn("value", new TableColumn(columnName))},
                    ResultReturnParameters = new List<Parameter>() {new ParameterOutputColumn("value", DataType.ETypeCode.String)},
                    ResultOutputs = new List<Parameter>() {new ParameterOutputColumn("distribution", DataType.ETypeCode.Unknown)},
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


		public (Transform sourceTransform, Table sourceTable) GetSourceTransform(DexihHub hub, DexihDatalinkTable hubDatalinkTable, InputColumn[] inputColumns, TransformWriterOptions transformWriterOptions)
		{
            try
            {
                Transform sourceTransform;
                Table sourceTable;
                
                switch (hubDatalinkTable.SourceType)
                {
                    case ESourceType.Datalink:
                        var datalink = hub.DexihDatalinks.SingleOrDefault(c => c.Key == hubDatalinkTable.SourceDatalinkKey);
                        if (datalink == null)
                        {
                            throw new TransformManagerException($"The source datalink with the key {hubDatalinkTable.SourceDatalinkKey} was not found");
                        }

                        (sourceTransform, sourceTable) = CreateRunPlan(hub, datalink, inputColumns, null, false, transformWriterOptions);
                        sourceTransform.ReferenceTableAlias = hubDatalinkTable.Key.ToString();
                        break;
                    case ESourceType.Table:
                        if (hubDatalinkTable.SourceTableKey == null)
                        {
                            throw new TransformManagerException($"The source table key was null.");
                        }
                        
                        var sourceDbTable = hub.GetTableFromKey(hubDatalinkTable.SourceTableKey.Value);
                        if (sourceDbTable == null)
                        {
                            throw new TransformManagerException($"The source table with the key {hubDatalinkTable.SourceTableKey.Value} could not be found.");
                        }
                        
                        var sourceDbConnection = hub.DexihConnections.SingleOrDefault(c => c.Key == sourceDbTable.ConnectionKey && c.IsValid);

                        if (sourceDbConnection == null)
                        {
                            throw new TransformException($"The connection with key {sourceDbTable.ConnectionKey} could not be found.");
                        }

                        var sourceConnection = sourceDbConnection.GetConnection(_transformSettings);
                        sourceTable = sourceDbTable.GetTable(hub, sourceConnection, inputColumns, _transformSettings);
                        sourceTransform = sourceConnection.GetTransformReader(sourceTable, transformWriterOptions.PreviewMode);
                        sourceTransform.ReferenceTableAlias = hubDatalinkTable.Key.ToString();

                        break;
                    case ESourceType.Rows:
                        var rowCreator = new ReaderRowCreator();
                        rowCreator.InitializeRowCreator(hubDatalinkTable.RowsStartAt??1, hubDatalinkTable.RowsEndAt??1, hubDatalinkTable.RowsIncrement??1);
                        rowCreator.ReferenceTableAlias = hubDatalinkTable.Key.ToString();
                        sourceTable = rowCreator.GetTable();
                        sourceTransform = rowCreator;
                        break;
                    case ESourceType.Function:
                        sourceTable = hubDatalinkTable.GetTable(null, inputColumns);
                        var data = new object[sourceTable.Columns.Count];
                        for(var i = 0; i< sourceTable.Columns.Count; i++)
                        {
                            data[i] = sourceTable.Columns[i].DefaultValue;
                        }
                        sourceTable.Data.Add(data);
                        var defaultRow = new ReaderDynamic(sourceTable);
                        defaultRow.Reset();
                        sourceTransform = defaultRow;
                        break;

                    default:
                        throw new TransformManagerException($"Error getting the source transform.");
                    
                }
                
                // compare the table in the transform to the source datalink columns.  If any are missing, add a mapping 
                // transform to include them.
                var transformColumns = sourceTransform.CacheTable.Columns;
                var datalinkColumns = hubDatalinkTable.DexihDatalinkColumns;


                // add a mapping transform to include inputColumns.
                var mappings = new Mappings();
                
                foreach (var column in datalinkColumns)
                {
                    var transformColumn = transformColumns.SingleOrDefault(c => c.Name == column.Name);
                    if (transformColumn == null)
                    {
                        var newColumn = column.GetTableColumn(inputColumns);
                        mappings.Add(new MapInputColumn(newColumn)); 
                    }
                }

                if (mappings.Count > 0)
                {
                    sourceTransform = new TransformMapping(sourceTransform, mappings)
                    {
                        Name = "Internal Mapping"
                    };
                }

                return (sourceTransform, sourceTable);
            }
            catch (Exception ex)
            {
                throw new TransformManagerException($"Get source transform failed.  {ex.Message}", ex);
            }
        }

        private void MergeInputColumn(TableColumn column, InputColumn[] inputColumns)
        {
            
        }

        public (Transform sourceTransform, Table sourceTable) CreateRunPlan(DexihHub hub, DexihDatalink hubDatalink, InputColumn[] inputColumns, long? maxDatalinkTransformKey, object maxIncrementalValue, TransformWriterOptions transformWriterOptions) //Last datatransform key is used to preview the output of a specific transform in the series.
        {
            try
            {
                _logger?.LogTrace($"CreateRunPlan {hubDatalink.Name} started.");

                var timer = Stopwatch.StartNew();
                
                if(transformWriterOptions == null) transformWriterOptions = new TransformWriterOptions();
                
				var primaryTransformResult = GetSourceTransform(hub, hubDatalink.SourceDatalinkTable, inputColumns, transformWriterOptions);
				var primaryTransform = primaryTransformResult.sourceTransform;
				var sourceTable = primaryTransformResult.sourceTable;
                var sourceDatalinkTableKey = hubDatalink.SourceDatalinkTable?.Key ?? hubDatalink.SourceDatalinkTableKey;
//				foreach(var column in primaryTransform.CacheTable.Columns)
//				{
//					column.ReferenceTable = sourceDatalinkTableKey.ToString();
//				}

                //add a filter for the incremental column (if there is one)
                var incrementalCol = sourceTable?.GetAutoIncrementColumn();
                var updateStrategy = hubDatalink.UpdateStrategy;

                if (maxDatalinkTransformKey == null && 
                    transformWriterOptions.IsEmptyTarget() == false && 
                    updateStrategy != TransformDelta.EUpdateStrategy.Reload && 
                    incrementalCol != null && 
                    (updateStrategy != TransformDelta.EUpdateStrategy.AppendUpdateDelete || updateStrategy != TransformDelta.EUpdateStrategy.AppendUpdateDeletePreserve) && 
                    maxIncrementalValue != null && 
                    maxIncrementalValue.ToString() != "")
                {

                    var mappings = new Mappings()
                    {
                        new MapFilter(incrementalCol, maxIncrementalValue, ECompare.GreaterThan)
                    };

                    var filterTransform = new TransformFilter(primaryTransform, mappings)
                    {
                        Name = $"Prefilter maxIncremental {maxIncrementalValue}"
                    };
                    filterTransform.SetInTransform(primaryTransform);
                    primaryTransform = filterTransform;
                }

                DexihTable targetTable = null;
                var target = hubDatalink.DexihDatalinkTargets.FirstOrDefault(c => c.NodeDatalinkColumnKey == null);
                if (target != null)
                {
                    targetTable = hub.GetTableFromKey(target.TableKey);
                }

                _logger?.LogTrace($"CreateRunPlan {hubDatalink.Name}.  Added incremental filter.  Elapsed: {timer.Elapsed}");
                
                //loop through the transforms to create the chain.
                foreach (var datalinkTransform in hubDatalink.DexihDatalinkTransforms.OrderBy(c => c.Position))
                {
                    //if this is an empty transform, then ignore it.
                    if (datalinkTransform.DexihDatalinkTransformItems.Count == 0)
                    {
                        if (datalinkTransform.TransformType == TransformAttribute.ETransformType.Filter || (datalinkTransform.TransformType == TransformAttribute.ETransformType.Mapping && datalinkTransform.PassThroughColumns))
                        {
                            if (datalinkTransform.Key == maxDatalinkTransformKey)
                                break;

                            continue;
                        }
                    }
                    
                    //if contains a join table, then add it in.
                    Transform referenceTransform = null;
                    if(datalinkTransform.JoinDatalinkTable != null) 
                    {
                        var joinTransformResult = GetSourceTransform(hub, datalinkTransform.JoinDatalinkTable, null, transformWriterOptions);
                        referenceTransform = joinTransformResult.sourceTransform;
                    }
                    
                    var transform = datalinkTransform.GetTransform(hub, transformWriterOptions.GlobalVariables, _transformSettings, primaryTransform, referenceTransform, targetTable, _logger);

                    _logger?.LogTrace($"CreateRunPlan {hubDatalink.Name}, adding transform {datalinkTransform.Name}.  Elapsed: {timer.Elapsed}");

                    primaryTransform = transform;

                    if (datalinkTransform.Key == maxDatalinkTransformKey)
                        break;
                }

                //if the maxDatalinkTransformKey is null (i.e. we are not doing a preview), and there are profiles add a profile transform.
                if (maxDatalinkTransformKey == null && hubDatalink.DexihDatalinkProfiles != null && hubDatalink.DexihDatalinkProfiles.Count > 0 && targetTable != null)
                {
                    var profileRules = new Mappings();
                    foreach (var profile in hubDatalink.DexihDatalinkProfiles)
                    {
                        foreach (var column in targetTable.DexihTableColumns.Where(c => c.IsSourceColumn))
                        {
                            var profileFunction = GetProfileFunction(profile.FunctionAssemblyName, profile.FunctionClassName, profile.FunctionMethodName, column.Name, profile.DetailedResults, transformWriterOptions.GlobalVariables);
                            profileRules.Add(profileFunction);
                        }
                    }
                    var transform = new TransformProfile(primaryTransform, profileRules)
                    {
                        Name =  "User defined profiles"
                    };

                    primaryTransform = transform;

                    _logger?.LogTrace($"CreateRunPlan {hubDatalink.Name}, adding profiling.  Elapsed: {timer.Elapsed}");
                }
                
                if(transformWriterOptions.SelectQuery != null)
                {
                    var transform = new TransformQuery(primaryTransform, transformWriterOptions.SelectQuery)
                    {
                        Name = "Select Query Filter"
                    };
                    primaryTransform = transform;
                }

                _logger?.LogTrace($"CreateRunPlan {hubDatalink.Name}, completed.  Elapsed: {timer.Elapsed}");

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

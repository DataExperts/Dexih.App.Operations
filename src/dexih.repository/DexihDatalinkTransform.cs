﻿using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using Dexih.Utils.CopyProperties;
using dexih.transforms;
using System.Linq;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using dexih.connections.sql;
using dexih.functions;
using dexih.operations;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;
using Dexih.Utils.DataType;
using Microsoft.Extensions.Logging;


namespace dexih.repository
{
    [DataContract]
    public class DexihDatalinkTransform : DexihHubNamedEntity
    {
		public DexihDatalinkTransform() => DexihDatalinkTransformItems = new HashSet<DexihDatalinkTransformItem>();

        [DataMember(Order = 7)]
        [CopyParentCollectionKey]
		public long DatalinkKey { get; set; }

        [DataMember(Order = 8)]
        public int Position { get; set; }

        [DataMember(Order = 9)]
        public bool PassThroughColumns { get; set; }

        [DataMember(Order = 10)]
        public long? JoinDatalinkTableKey { get; set; }

        [DataMember(Order = 11)]
        public long? JoinSortDatalinkColumnKey { get; set; }

        [DataMember(Order = 12)]
        public long? NodeDatalinkColumnKey { get; set; }

        [DataMember(Order = 13)]
        public ETransformType TransformType { get; set; }

        [DataMember(Order = 14)]
        public string TransformClassName { get; set; }

        [DataMember(Order = 15)]
        public string TransformAssemblyName { get; set; }

        [DataMember(Order = 16)] 
        public EJoinStrategy JoinStrategy { get; set; } = EJoinStrategy.Auto;

        [DataMember(Order = 17)] 
        public EDuplicateStrategy JoinDuplicateStrategy { get; set; } = EDuplicateStrategy.All;

        [DataMember(Order = 18)]
        public EJoinNotFoundStrategy JoinNotFoundStrategy { get; set; } = EJoinNotFoundStrategy.Filter;

        [DataMember(Order = 19)]
        [NotMapped]
        public EntityStatus EntityStatus { get; set; }

        [DataMember(Order = 20)]
        public ICollection<DexihDatalinkTransformItem> DexihDatalinkTransformItems { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihDatalink Datalink { get; set; }

        [DataMember(Order = 21)]
        public DexihDatalinkTable JoinDatalinkTable { get; set; }

        [DataMember(Order = 22)]
        public DexihDatalinkColumn JoinSortDatalinkColumn { get; set; }

        [DataMember(Order = 23)]
        public DexihDatalinkColumn NodeDatalinkColumn { get; set; }

        [DataMember(Order = 24)]
        public long MaxInputRows { get; set; } = 0;

        [DataMember(Order = 25)]
        public long MaxOutputRows { get; set; } = 0;

        /// <summary>
        /// Cache data to storage prior to running transform, to reduce high memory usage with sort, group, join queries.
        /// </summary>
        [DataMember(Order = 26)] 
        public bool DataCache { get; set; } = false;

        [DataMember(Order = 27)] 
        public long? DataCacheConnectionKey { get; set; }
        
        [JsonIgnore, IgnoreDataMember, CopyReference]
        public DexihConnection DataCacheConnection { get; set; }
        
        public override void ResetKeys()
        {
            base.ResetKeys();
            
            
            JoinDatalinkTable?.ResetKeys();
            JoinSortDatalinkColumn?.ResetKeys();
            NodeDatalinkColumn?.ResetKeys();
            
            
            foreach (var item in DexihDatalinkTransformItems)
            {
                item.ResetKeys();
            }
        }
        
        /// <summary>
        /// Gets all the mapped output columns for this transform.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<DexihDatalinkColumn> GetOutputColumns()
        {
            var columns = new List<DexihDatalinkColumn>();
            foreach(var item in DexihDatalinkTransformItems)
            {
                if(item.TargetDatalinkColumn != null)
                {
                    columns.Add(item.TargetDatalinkColumn);
                }

                foreach(var param in item.DexihFunctionParameters)
                {
                    if(param.IsOutput() && param.DatalinkColumn != null)
                    {
                        columns.Add(param.DatalinkColumn);
                    }
                }
            }

            return columns;
        }


        /// <summary>
        /// Get's all mapped source/input columns for this transform.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<DexihDatalinkColumn> GetInputColumns()
        {
            var columns = new List<DexihDatalinkColumn>();
            foreach (var item in DexihDatalinkTransformItems)
            {
                if (item.SourceDatalinkColumn != null)
                {
                    columns.Add(item.SourceDatalinkColumn);
                }

                foreach (var param in item.DexihFunctionParameters)
                {
                    if (param.IsInput() && param.DatalinkColumn != null)
                    {
                        columns.Add(param.DatalinkColumn);
                    }
                }
            }

            return columns;
        }

        public Transform GetTransform(DexihHub hub, DexihDatalink datalink, GlobalSettings globalSettings, TransformSettings transformSettings, Transform primaryTransform, Transform referenceTransform, DexihTable targetTable, ILogger logger = null)
        {
            try
            {
                var timer = Stopwatch.StartNew();
                logger?.LogTrace($"GetTransform {Name}, started.");
                
                var transformReference = Transforms.GetTransform(TransformClassName, TransformAssemblyName);
                var transform = transformReference.GetTransform();

                logger?.LogTrace($"GetTransform {Name}, get transform object.  Elapsed: {timer.Elapsed}");

                if(!string.IsNullOrEmpty(Name))
				{
					transform.Name = Name;
				}
                else
                {
                    transform.Name = "Transform - " + Key;
                }

                transform.MaxInputRows = MaxInputRows;
                transform.MaxOutputRows = MaxOutputRows;
                transform.TableAlias = datalink.SourceDatalinkTableKey.ToString();

				// if(JoinDatalinkTable != null)
				// {
				// 	transform.TableAlias = JoinDatalinkTable.Key.ToString();	
				// }

                var mappings = new Mappings(PassThroughColumns);

                foreach (var item in DexihDatalinkTransformItems.OrderBy(c => c.Position).Where(c => c.IsValid))
                {
                    logger?.LogTrace($"GetTransform {Name}, get item.  Elapsed: {timer.Elapsed}");

                    if (transformSettings.HasVariables())
                    {
                        if (!string.IsNullOrEmpty(item.SourceValue))
                            item.SourceValue = transformSettings.InsertHubVariables(item.SourceValue);
                        if (!string.IsNullOrEmpty(item.SeriesStart))
                            item.SeriesStart = transformSettings.InsertHubVariables(item.SeriesStart);
                        if (!string.IsNullOrEmpty(item.SeriesFinish))
                            item.SeriesFinish = transformSettings.InsertHubVariables(item.SeriesFinish);
                        if (!string.IsNullOrEmpty(item.SeriesProject))
                            item.SeriesProject = transformSettings.InsertHubVariables(item.SeriesProject);
                        if (!string.IsNullOrEmpty(item.SeriesStep))
                            item.SeriesStep = transformSettings.InsertHubVariables(item.SeriesStep);
                        
                        foreach (var param in item.DexihFunctionParameters)
                        {
                            if (!string.IsNullOrEmpty(param.Value))
                                param.Value = transformSettings.InsertHubVariables(param.Value);

                            foreach (var arrayParam in param.ArrayParameters)
                            {
                                if (!string.IsNullOrEmpty(arrayParam.Value))
                                    arrayParam.Value = transformSettings.InsertHubVariables(arrayParam.Value);
                            }
                        }
                    }

                    var sourceColumn = item.SourceDatalinkColumn?.GetTableColumn(null);
					var targetColumn = item.TargetDatalinkColumn?.GetTableColumn(null);
					var joinColumn = item.JoinDatalinkColumn?.GetTableColumn(null);
                    var filterColumn = item.FilterDatalinkColumn?.GetTableColumn(null);

                    switch (item.TransformItemType)
                    {
                        case ETransformItemType.ColumnPair:
                            if (targetColumn == null)
                            {
                                throw new RepositoryException("The target column with the key " + item.TargetDatalinkColumnKey + " and the source " + (sourceColumn == null ? "Unknown" : sourceColumn.Name) + " had an error.  Please review the mappings and fix any errors.");
                            }

                            mappings.Add(new MapColumn(item.SourceValue, sourceColumn, targetColumn));
                            break;
                        case ETransformItemType.JoinPair:
                            var joinOp = item.FilterCompare ?? ECompare.IsEqual;
                            var joinValue = transformSettings.UpdateHubVariable(item.JoinValue, joinOp == ECompare.IsIn ? 1 : 0);
                            mappings.Add(new MapJoin(item.SourceValue, sourceColumn, joinValue, joinColumn, joinOp));
                            break;
                        case ETransformItemType.FilterPair:
                            var op = item.FilterCompare ?? ECompare.IsEqual;
                            var filterValue = transformSettings.UpdateHubVariable(item.FilterValue, op == ECompare.IsIn ? 1 : 0);
                            
                            mappings.Add(new MapFilter()
                            {
                                Column1 = sourceColumn,
                                Column2 = filterColumn,
                                Operator = op, 
                                Value1 = item.SourceValue,
                                Value2 = filterValue
                            });
                            break;
                        case ETransformItemType.AggregatePair:
                            mappings.Add(new MapAggregate(sourceColumn, targetColumn, item.Aggregate??EAggregate.Sum)
                            {
                                Value = item.SourceValue
                            });
                            break;
                        case ETransformItemType.BuiltInFunction:
                        case ETransformItemType.CustomFunction:
                            var func = item.CreateFunctionMethod(hub, globalSettings, false, logger);
                            func.function.FunctionType = TransformType switch
                            {
                                ETransformType.Aggregate => EFunctionType.Aggregate,
                                ETransformType.Series => EFunctionType.Series,
                                ETransformType.Filter => EFunctionType.Condition,
                                ETransformType.Group => EFunctionType.Aggregate,
                                ETransformType.Rows => EFunctionType.Rows,
                                _ => EFunctionType.Map
                            };

                            if (TransformType == ETransformType.Validation)
                            {
                                mappings.Add(new MapValidation(func.function, func.parameters));
                            }
                            else
                            {
                                mappings.Add(new MapFunction(func.function, func.parameters, item.FunctionCaching));    
                            }
                            
                            break;
                        case ETransformItemType.Sort:
                            if (sourceColumn == null)
                            {
                                throw new RepositoryException($"The sort item in transform {Name} does not have a column specified.");
                            }
                            
                            mappings.Add(new MapSort(sourceColumn, item.SortDirection ?? ESortDirection.Ascending));
                            break;
                        case ETransformItemType.Column:
                            mappings.Add(new MapGroup(sourceColumn));
                            break;
                        case ETransformItemType.Series:
                            if (sourceColumn == null)
                            {
                                throw new RepositoryException($"The series transform {Name} does not have a series column specified.");
                            }

                            if (item.SeriesGrain == null)
                            {
                                throw new RepositoryException($"The series transform {Name} does not have a series grain specified.");
                            }

                            int seriesProject = 0;
                            if (!string.IsNullOrEmpty(item.SeriesProject))
                            {
                                if (!int.TryParse(item.SeriesProject, out seriesProject))
                                {
                                    throw new RepositoryException($"The series transform {Name} requires a numeric value or a valid variable for the series projections.  The current value is {item.SeriesProject}.");                                    
                                }
                            }
                            
                            int seriesStep = 1;
                            if (!string.IsNullOrEmpty(item.SeriesStep))
                            {
                                if (!int.TryParse(item.SeriesStep, out seriesStep))
                                {
                                    throw new RepositoryException($"The series transform {Name} requires a numeric value or a valid variable for the series step.  The current value is {item.SeriesProject}.");                                    
                                }
                            }

                            mappings.Add(new MapSeries(
                                sourceColumn, 
                                item.SeriesGrain??ESeriesGrain.Day,
                                seriesStep,
                                item.SeriesFill, 
                                item.SeriesStart, 
                                item.SeriesFinish,
                                seriesProject));
                            break;
                        case ETransformItemType.JoinNode:
                            if (targetColumn == null)
                            {
                                throw new RepositoryException("The node column with the key " + item.TargetDatalinkColumnKey + " had an error.  Please review the mappings and fix any errors.");
                            }

                            var joinTable = JoinDatalinkTable.GetTable(null, null);
                            mappings.Add(new MapJoinNode(targetColumn, joinTable));
                            break;
                        case ETransformItemType.GroupNode:
                            if (targetColumn == null)
                            {
                                throw new RepositoryException("The node column with the key " + item.TargetDatalinkColumnKey + " had an error.  Please review the mappings and fix any errors.");
                            }
                            mappings.Add(new MapGroupNode(targetColumn));
                            break;
                        case ETransformItemType.UnGroup:
                            var columns = item.DexihFunctionParameters.Select(c => c.DatalinkColumn.GetTableColumn(null)).ToArray();
                            mappings.Add(new MapUnGroup(sourceColumn, columns));
                            break;
                    }
                }

                // transform.PassThroughColumns = PassThroughColumns;
                transform.JoinStrategy = JoinStrategy;
                transform.JoinDuplicateStrategy = JoinDuplicateStrategy;
                transform.JoinNotFoundStrategy = JoinNotFoundStrategy;
                
                var joinSortColumn = JoinSortDatalinkColumn?.GetTableColumn(null);
                transform.JoinSortField = joinSortColumn;

                //if this is a validation transform. add the column validations also.
                if (TransformType == ETransformType.Validation)
                {
                    if(targetTable == null)
                    {
                        throw new RepositoryException($"The validation transform failed, as a valid target table was not set.");
                    }

                    foreach (var column in targetTable.DexihTableColumns.Where(c => c.ColumnValidationKey != null))
                    {
                        var columnValidation = hub.DexihColumnValidations.Single(c => c.IsValid && c.Key == column.ColumnValidationKey);
                        var validation =
                            new ColumnValidationRun(transformSettings, columnValidation, hub)
                            {
                                DefaultValue = column.DefaultValue
                            };
                        var function = validation.GetValidationMapping(column.Name);
                        mappings.Add(function);
                    }

                    logger?.LogTrace($"Adding validation.  Elapsed: {timer.Elapsed}");
                }

                if (NodeDatalinkColumn != null)
                {
                    var parentTransform = primaryTransform;
                    var parentTable = primaryTransform.CacheTable;

                    // create a path through hierarchy from the parent to the child column.
                    var columnPath = FindNodeColumnPath(NodeDatalinkColumn, parentTable.Columns);

//                    var currentColumn = columnPath[0];
//                    var nodeColumn = parentTable.Columns[currentColumn.Name, currentColumn.ColumnGroup];
                    transform = transform.CreateNodeMapping(parentTransform, referenceTransform, mappings, columnPath);
                }
                else
                {
                    transform.Mappings = mappings;    
                    
                    if (DataCache)
                    {
                        var cacheDexihConnection = hub.DexihConnections.SingleOrDefault(c => c.Key == DataCacheConnectionKey && c.IsValid);
                        if (cacheDexihConnection == null)
                        {
                            throw new Exception($"The data cache should not be set as the connection with the key {DataCacheConnectionKey} could not be found.");
                        }

                        var cacheConnection = cacheDexihConnection.GetConnection(transformSettings);
                        
                        if (cacheConnection is ConnectionSql connectionSql)
                        {
                            var dataCache = new TransformStorageCache(primaryTransform, connectionSql);
                            
                            if (referenceTransform != null)
                            {
                                referenceTransform = new TransformStorageCache(referenceTransform, connectionSql);
                            }
                            
                            transform.SetInTransform(dataCache, referenceTransform);
                        }
                        else
                        {
                            throw new Exception($"The data cache should not be set as the connection {cacheConnection.Name} is not a SQL based connection");
                        }
                    }
                    else
                    {
                        transform.SetInTransform(primaryTransform, referenceTransform);    
                    }
                }

                logger?.LogTrace($"GetTransform {Name}, finished.  Elapsed: {timer.Elapsed}");

                return transform;
            }

            catch (Exception ex)
            {
				throw new RepositoryException($"Failed to construct the transform {Name}.  {ex.Message}", ex);
            }
        }
        
        // used to find a node column within a node structure.
        private TableColumn[] FindNodeColumnPath(DexihDatalinkColumn datalinkColumn, TableColumns columns) {
            if (datalinkColumn == null || columns == null || !columns.Any()) {
                return null;
            }

            var column = datalinkColumn.GetTableColumn(null);

            var nodeColumn = columns[column];
            if (nodeColumn != null)
            {
                return new [] { nodeColumn };
            }
            
            foreach(var col in columns) {
                if (col.ChildColumns != null) {
                    var returnCol = FindNodeColumnPath(datalinkColumn, col.ChildColumns);
                    if (returnCol != null) {
                        return returnCol.Prepend(col).ToArray();
                    }
                }
            }

            return null;
        }

    }
}


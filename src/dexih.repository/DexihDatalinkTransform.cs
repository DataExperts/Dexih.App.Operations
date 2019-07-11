using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using static dexih.transforms.Transform;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using Dexih.Utils.CopyProperties;
using dexih.transforms;
using System.Linq;
using dexih.functions;
using dexih.functions.Query;
using dexih.operations;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;
using Microsoft.Extensions.Logging;
using static dexih.transforms.Transforms.TransformAttribute;

namespace dexih.repository
{
    [Serializable]
    public class DexihDatalinkTransform : DexihHubNamedEntity
    {
		public DexihDatalinkTransform() => DexihDatalinkTransformItems = new HashSet<DexihDatalinkTransformItem>();



        [CopyParentCollectionKey]
		public long DatalinkKey { get; set; }
        public int Position { get; set; }
        
        public bool PassThroughColumns { get; set; }

        public long? JoinDatalinkTableKey { get; set; }
        public long? JoinSortDatalinkColumnKey { get; set; }
        
        public long? NodeDatalinkColumnKey { get; set; }

        public ETransformType TransformType { get; set; }

        public string TransformClassName { get; set; }
        public string TransformAssemblyName { get; set; }

        public EDuplicateStrategy JoinDuplicateStrategy { get; set; }

        [NotMapped]
        public EntityStatus EntityStatus { get; set; }

        public ICollection<DexihDatalinkTransformItem> DexihDatalinkTransformItems { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDatalink Datalink { get; set; }

        public DexihDatalinkTable JoinDatalinkTable { get; set; }
        public DexihDatalinkColumn JoinSortDatalinkColumn { get; set; }
        
        public DexihDatalinkColumn NodeDatalinkColumn { get; set; }

        public long MaxInputRows { get; set; } = 0;
        public long MaxOutputRows { get; set; } = 0;

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

        public Transform GetTransform(DexihHub hub, GlobalVariables globalVariables, TransformSettings transformSettings, Transform primaryTransform, Transform referenceTransform, DexihTable targetTable, ILogger logger = null)
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

				if(JoinDatalinkTable != null)
				{
					transform.ReferenceTableAlias = JoinDatalinkTable.Key.ToString();	
				}

                var mappings = new Mappings(PassThroughColumns);

                foreach (var item in DexihDatalinkTransformItems.OrderBy(c => c.Position))
                {
                    logger?.LogTrace($"GetTransform {Name}, get item.  Elapsed: {timer.Elapsed}");

                    if (transformSettings.HasVariables())
                    {
                        if (!string.IsNullOrEmpty(item.FilterValue))
                            item.FilterValue = transformSettings.InsertHubVariables(item.FilterValue, false);
                        if (!string.IsNullOrEmpty(item.JoinValue))
                            item.JoinValue = transformSettings.InsertHubVariables(item.JoinValue, false);
                        if (!string.IsNullOrEmpty(item.SourceValue))
                            item.SourceValue = transformSettings.InsertHubVariables(item.SourceValue, false);
                        if (!string.IsNullOrEmpty(item.SeriesStart))
                            item.SeriesStart = transformSettings.InsertHubVariables(item.SeriesStart, false);
                        if (!string.IsNullOrEmpty(item.SeriesFinish))
                            item.SeriesFinish = transformSettings.InsertHubVariables(item.SeriesFinish, false);


                        foreach (var param in item.DexihFunctionParameters)
                        {
                            if (!string.IsNullOrEmpty(param.Value))
                                param.Value = transformSettings.InsertHubVariables(param.Value, false);

                            foreach (var arrayParam in param.ArrayParameters)
                            {
                                if (!string.IsNullOrEmpty(arrayParam.Value))
                                    arrayParam.Value = transformSettings.InsertHubVariables(arrayParam.Value, false);
                            }
                        }
                    }

                    var sourceColumn = item.SourceDatalinkColumn?.GetTableColumn(null);
					var targetColumn = item.TargetDatalinkColumn?.GetTableColumn(null);
					var joinColumn = item.JoinDatalinkColumn?.GetTableColumn(null);
                    var filterColumn = item.FilterDatalinkColumn?.GetTableColumn(null);

                    switch (item.TransformItemType)
                    {
                        case DexihDatalinkTransformItem.ETransformItemType.ColumnPair:
                            if (targetColumn == null)
                            {
                                throw new RepositoryException("The target column with the key " + item.TargetDatalinkColumnKey + " and the source " + (sourceColumn == null ? "Unknown" : sourceColumn.Name) + " had an error.  Please review the mappings and fix any errors.");
                            }

                            mappings.Add(new MapColumn(item.SourceValue, sourceColumn, targetColumn));
                            break;
                        case DexihDatalinkTransformItem.ETransformItemType.JoinPair:
                            
                            mappings.Add(new MapJoin(item.SourceValue, sourceColumn, item.JoinValue, joinColumn));
                            break;
                        case DexihDatalinkTransformItem.ETransformItemType.FilterPair:
                            mappings.Add(new MapFilter()
                            {
                                Column1 = sourceColumn,
                                Column2 = filterColumn,
                                Compare = item.FilterCompare??Filter.ECompare.IsEqual, 
                                Value1 = item.SourceValue,
                                Value2 = item.FilterValue
                            });
                            break;
                        case DexihDatalinkTransformItem.ETransformItemType.AggregatePair:
                            mappings.Add(new MapAggregate(sourceColumn, targetColumn, item.Aggregate??SelectColumn.EAggregate.Sum)
                            {
                                Value = item.SourceValue
                            });
                            break;
                        case DexihDatalinkTransformItem.ETransformItemType.BuiltInFunction:
                        case DexihDatalinkTransformItem.ETransformItemType.CustomFunction:
                            var func = item.CreateFunctionMethod(hub, globalVariables, false, logger);
                            if (TransformType == ETransformType.Validation)
                            {
                                mappings.Add(new MapValidation(func.function, func.parameters));
                            }
                            else
                            {
                                mappings.Add(new MapFunction(func.function, func.parameters, item.FunctionCaching));    
                            }
                            
                            break;
                        case DexihDatalinkTransformItem.ETransformItemType.Sort:
                            mappings.Add(new MapSort(sourceColumn, item.SortDirection ?? Sort.EDirection.Ascending));
                            break;
                        case DexihDatalinkTransformItem.ETransformItemType.Column:
                            mappings.Add(new MapGroup(sourceColumn));
                            break;
                        case DexihDatalinkTransformItem.ETransformItemType.Series:
                            mappings.Add(new MapSeries(
                                sourceColumn, 
                                item.SeriesGrain??ESeriesGrain.Day, 
                                item.SeriesFill, 
                                item.SeriesStart, 
                                item.SeriesFinish));
                            break;
                        case DexihDatalinkTransformItem.ETransformItemType.JoinNode:
                            if (targetColumn == null)
                            {
                                throw new RepositoryException("The node column with the key " + item.TargetDatalinkColumnKey + " had an error.  Please review the mappings and fix any errors.");
                            }

                            var joinTable = JoinDatalinkTable.GetTable(null, null);
                            mappings.Add(new MapJoinNode(targetColumn, joinTable));
                            break;
                        case DexihDatalinkTransformItem.ETransformItemType.GroupNode:
                            if (targetColumn == null)
                            {
                                throw new RepositoryException("The node column with the key " + item.TargetDatalinkColumnKey + " had an error.  Please review the mappings and fix any errors.");
                            }
                            mappings.Add(new MapGroupNode(targetColumn));
                            break;
                        case DexihDatalinkTransformItem.ETransformItemType.UnGroup:
                            var columns = item.DexihFunctionParameters.Select(c => c.DatalinkColumn.GetTableColumn(null)).ToArray();
                            mappings.Add(new MapUnGroup(sourceColumn, columns));
                            break;
                    }
                }

                
                // transform.PassThroughColumns = PassThroughColumns;
                transform.JoinDuplicateStrategy = JoinDuplicateStrategy;
                
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
                        var columnValidation = hub.DexihColumnValidations.Single(c => c.Key == column.ColumnValidationKey);
                        var validation =
                            new ColumnValidationRun(transformSettings, columnValidation, hub)
                            {
                                DefaultValue = column.DefaultValue
                            };
                        var function = validation.GetValidationMapping(column.Name);
                        transform.Mappings.Add(function);
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
                    transform.SetInTransform(primaryTransform, referenceTransform);
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


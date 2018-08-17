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
using dexih.transforms.Transforms;
using Microsoft.Extensions.Logging;
using static dexih.transforms.Transforms.TransformAttribute;

namespace dexih.repository
{
    public class DexihDatalinkTransform : DexihBaseEntity
    {
		public DexihDatalinkTransform() => DexihDatalinkTransformItems = new HashSet<DexihDatalinkTransformItem>();

		[JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }

        [CopyCollectionKey((long)0, true)]
        public long DatalinkTransformKey { get; set; }

        [CopyParentCollectionKey]
		public long DatalinkKey { get; set; }
        public int Position { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public bool PassThroughColumns { get; set; }

        public long? JoinDatalinkTableKey { get; set; }
        public string JoinTableAlias { get; set; }
        public long? JoinSortDatalinkColumnKey { get; set; }

        [NotMapped]
        public ETransformType TransformType { get; set; }

        [JsonIgnore, CopyIgnore]
        public string TransformTypeString
        {
            get => TransformType.ToString();
            set => TransformType = (ETransformType)Enum.Parse(typeof(ETransformType), value);
        }

        public string TransformClassName { get; set; }
        public string TransformAssemblyName { get; set; }

        [NotMapped]
        public EDuplicateStrategy JoinDuplicateStrategy { get; set; }

        [JsonIgnore, CopyIgnore]
        public string JoinDuplicateStrategyString {
            get => JoinDuplicateStrategy.ToString();
            set => JoinDuplicateStrategy = (EDuplicateStrategy) Enum.Parse(typeof(EDuplicateStrategy), value);
        }

        [NotMapped]
        public EntityStatus EntityStatus { get; set; }

        public virtual ICollection<DexihDatalinkTransformItem> DexihDatalinkTransformItems { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihDatalink Datalink { get; set; }

        public virtual DexihDatalinkTable JoinDatalinkTable { get; set; }
        public virtual DexihDatalinkColumn JoinSortDatalinkColumn { get; set; }

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
                    if(param.Direction == DexihParameterBase.EParameterDirection.Output && param.DatalinkColumn != null)
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
                    if (param.Direction == DexihParameterBase.EParameterDirection.Input && param.DatalinkColumn != null)
                    {
                        columns.Add(param.DatalinkColumn);
                    }
                }
            }

            return columns;
        }

        public Transform GetTransform(DexihHub hub, GlobalVariables globalVariables, ILogger logger = null)
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
                    transform.Name = "Transfom - " + DatalinkTransformKey;
                }

				if(JoinDatalinkTable != null)
				{
					transform.ReferenceTableAlias = JoinDatalinkTable.DatalinkTableKey.ToString();	
				}

                foreach (var item in DexihDatalinkTransformItems.OrderBy(c => c.Position))
                {
                    logger?.LogTrace($"GetTransform {Name}, get item.  Elapsed: {timer.Elapsed}");

                    var sourceColumn = item.SourceDatalinkColumn?.GetTableColumn(null);
					var targetColumn = item.TargetDatalinkColumn?.GetTableColumn(null);
					var joinColumn = item.JoinDatalinkColumn?.GetTableColumn(null);
                    var filterColumn = item.FilterDatalinkColumn?.GetTableColumn(null);

                    switch (item.TransformItemType)
                    {
                        case DexihDatalinkTransformItem.ETransformItemType.ColumnPair:

                            if (sourceColumn == null)
                            {
                                throw new RepositoryException("The source column with the key " + item.SourceDatalinkColumnKey + " and the target " +  (targetColumn == null ? "Unknown" : targetColumn.Name) + " had an error.  Plese review the mappings and fix any errors.");
                            }

                            if (targetColumn == null)
                            {
                                throw new RepositoryException("The target column with the key " + item.TargetDatalinkColumnKey + " and the source " + (sourceColumn == null ? "Unknown" : sourceColumn.Name) + " had an error.  Plese review the mappings and fix any errors.");
                            }

                            transform.ColumnPairs.Add(new ColumnPair(sourceColumn, targetColumn));

                            break;
                        case DexihDatalinkTransformItem.ETransformItemType.JoinPair:
                            transform.JoinPairs.Add(new JoinPair()
                            {
                                SourceColumn = sourceColumn,
                                JoinColumn = joinColumn,
                                JoinValue = item.JoinValue
                            });

                            break;
                        case DexihDatalinkTransformItem.ETransformItemType.FilterPair:
                            transform.FilterPairs.Add(new FilterPair()
                            {
                                Column1 = sourceColumn,
                                Column2 = filterColumn,
                                Compare = item.FilterCompare??Filter.ECompare.IsEqual, 
                                FilterValue = item.FilterValue
                            });

                            break;
                        case DexihDatalinkTransformItem.ETransformItemType.AggregatePair:
                            transform.AggregatePairs.Add(new AggregatePair()
                            {
                                SourceColumn = sourceColumn,
                                TargetColumn = targetColumn,
                                Aggregate = item.Aggregate??SelectColumn.EAggregate.Sum
                            });

                            break;
                        case DexihDatalinkTransformItem.ETransformItemType.BuiltInFunction:
                        case DexihDatalinkTransformItem.ETransformItemType.CustomFunction:
                            var createNewFunction = item.CreateFunctionMethod(hub, globalVariables, false, logger);

                            transform.Functions.Add(createNewFunction);

                            break;
                        case DexihDatalinkTransformItem.ETransformItemType.Sort:
                            transform.SortFields.Add(new Sort() { Column = sourceColumn, Direction = item.SortDirection ?? Sort.EDirection.Ascending });

                            break;
                        case DexihDatalinkTransformItem.ETransformItemType.Column:
                            transform.ColumnPairs.Add(new ColumnPair(sourceColumn));

                            break;
                    }
                }

                transform.PassThroughColumns = PassThroughColumns;
                transform.JoinDuplicateStrategy = JoinDuplicateStrategy;
                
                var joinSortColumn = JoinSortDatalinkColumn?.GetTableColumn(null);
                transform.JoinSortField = joinSortColumn;

                logger?.LogTrace($"GetTransform {Name}, finished.  Elapsed: {timer.Elapsed}");

                return transform;
            }

            catch (Exception ex)
            {
				throw new RepositoryException($"Failed to construct the transform {Name}.  {ex.Message}", ex);
            }
        }


    }
}


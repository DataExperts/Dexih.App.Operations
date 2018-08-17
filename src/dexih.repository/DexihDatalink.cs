using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations.Schema;
using Newtonsoft.Json.Converters;
using Dexih.Utils.CopyProperties;
using System.Linq;
using dexih.transforms;
using dexih.transforms.Transforms;

namespace dexih.repository
{
    public partial class DexihDatalink : DexihBaseEntity
    {


        [JsonConverter(typeof(StringEnumConverter))]
        public enum EDatalinkType
        {
            General,
            Stage,
            Validate,
            Transform,
            Deliver,
            Publish,
            Share,
            Query
        }

        public DexihDatalink()
        {
            DexihDatalinkProfiles = new HashSet<DexihDatalinkProfile>();
            DexihDatalinkSteps = new HashSet<DexihDatalinkStep>();
            DexihDatalinkTransforms = new HashSet<DexihDatalinkTransform>();
            EntityStatus = new EntityStatus();
        }

		[CopyCollectionKey((long)0, true)]
		public long DatalinkKey { get; set; }

        public long HubKey { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public long SourceDatalinkTableKey { get; set; }
        public long? TargetTableKey { get; set; }
        public long? AuditConnectionKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public string UpdateStrategyString
        {
            get => UpdateStrategy.ToString();
            set => UpdateStrategy = value == null ? TransformDelta.EUpdateStrategy.Reload  : (TransformDelta.EUpdateStrategy)Enum.Parse(typeof(TransformDelta.EUpdateStrategy), value);
        }

        [NotMapped]
        public TransformDelta.EUpdateStrategy UpdateStrategy { get; set; }

        public bool VirtualTargetTable { get; set; }

        [NotMapped]
        public EDatalinkType DatalinkType { get; set; }

        [JsonIgnore, CopyIgnore]
        public string DatalinkTypeString
        {
            get => DatalinkType.ToString();
	        set => DatalinkType = (EDatalinkType)Enum.Parse(typeof(EDatalinkType), value);
        }

        public int RowsPerCommit { get; set; }
        public int RowsPerProgress { get; set; }
        public bool RollbackOnFail { get; set; }
        public bool NoDataload { get; set; }
        public int MaxRows { get; set; }
        public bool AddDefaultRow { get; set; }
        public string ProfileTableName { get; set; }
		public bool IsShared { get; set; }

        [NotMapped]
        public EntityStatus EntityStatus { get; set; }

        //[CopyReference]
        public virtual ICollection<DexihDatalinkProfile> DexihDatalinkProfiles { get; set; }
        public virtual ICollection<DexihDatalinkTransform> DexihDatalinkTransforms { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual ICollection<DexihDatalinkTable> DexihDatalinkTables { get; set; }


        /// <summary>
        /// Reference to the source columns for the datalink.
        /// </summary>
        public virtual DexihDatalinkTable SourceDatalinkTable { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual ICollection<DexihDatalinkStep> DexihDatalinkSteps { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public virtual DexihHub Hub { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihTable TargetTable { get; set; }
        
	    [JsonIgnore, CopyReference]
        public virtual DexihConnection AuditConnection { get; set; }

        /// <summary>
        /// Gets the output columns (including join/passthrough columns for a transform).
        /// If the datalinkTransform is null, this will be output columns for the datalink.
        /// </summary>
        /// <param name="datalinkTransform"></param>
        /// <returns></returns>
        public List<DexihDatalinkTable> GetOutputColumns(DexihDatalinkTransform datalinkTransform = null)
        {
            // if there is no datalinkTransform set, assume we are looking at the last transform.
            int position;
            if (datalinkTransform != null)
            {
                position = datalinkTransform.Position;
            }
            else
            {
                position = int.MaxValue;
            }

            // get a reverse sorted list of transforms prior to the current one.
            var transforms = DexihDatalinkTransforms
                .Where(t => t.Position < position)
                .OrderByDescending(p => p.Position).ToArray();

            var inputTables = new List<DexihDatalinkTable>();

            if (transforms.Any())
            {
                var transform = transforms[0];

                if (transform.PassThroughColumns)
                {
                    inputTables = GetOutputColumns(transform);
                }

                var newMappingsTable = inputTables.SingleOrDefault(c => c.SourceTableKey == -987654321);

                if (newMappingsTable == null)
                {
                    // create a temporary table to use for any mapped columns in previous transforms.
                    newMappingsTable = new DexihDatalinkTable
                    {
                        SourceTableKey = -987654321,
                        Name = "Mappings Outputs",
                        DexihDatalinkColumns = new List<DexihDatalinkColumn>()
                    };
                    inputTables.Insert(0, newMappingsTable);
                }

                // add any columns in the transform table that are not already included.
                foreach (var column in transform.GetOutputColumns())
                {
                    newMappingsTable.DexihDatalinkColumns.Add(column);
                }

                // if the transform is a join, then add the join table columns
                if (transform.TransformType == TransformAttribute.ETransformType.Join)
                {
                    inputTables.Add(transform.JoinDatalinkTable);
                }

                // if the transform is a concatenate, then merge common column names together.
                if (transform.TransformType == TransformAttribute.ETransformType.Concatenate)
                {
                    var joinTable = transform.JoinDatalinkTable;

                    var concatTable = new DexihDatalinkTable
                    {
                        Name = "Concatenated Table",
                        DexihDatalinkColumns = new List<DexihDatalinkColumn>()
                    };

                    foreach (var table in inputTables)
                    {
                        foreach (var column in table.DexihDatalinkColumns)
                        {
                            if (concatTable.DexihDatalinkColumns.SingleOrDefault(c => c.Name == column.Name) == null)
                            {
                                concatTable.DexihDatalinkColumns.Add(column);
                            }
                        }
                    }

                    foreach (var column in joinTable.DexihDatalinkColumns)
                    {
                        if (concatTable.DexihDatalinkColumns.SingleOrDefault(c => c.Name == column.Name) == null)
                        {
                            concatTable.DexihDatalinkColumns.Add(column);
                        }
                    }

                    inputTables = new List<DexihDatalinkTable>() { concatTable };
                }

            }
            else
            {
                inputTables = new List<DexihDatalinkTable>() { SourceDatalinkTable };
            }

            return inputTables;
        }

        /// <summary>
        /// Gets DatalinkTable containing a flattened set of columns.  
        /// </summary>
        /// <returns></returns>
        public DexihDatalinkTable GetOutputTable()
        {
            var outputTables = GetOutputColumns();

            var dbTable = new DexihDatalinkTable {Name = Name};

            //flatten the datalink outputs into one table, removing any duplicate names.
            var columns = new Dictionary<string, DexihDatalinkColumn>();
            foreach (var t in outputTables)
            {
                foreach (var c in t.DexihDatalinkColumns)
                {
                    if (!columns.ContainsKey(c.Name))
                    {
                        columns.Add(c.Name, c);
                    }
                }
            }

            dbTable.DexihDatalinkColumns = columns.Values;

            return dbTable;
        }

  
        /// <summary>
        /// Gets a single (deduplicated) list of all DatalinkColumns use by the datalink.
        /// </summary>
        /// <returns></returns>
        public Dictionary<long, DexihDatalinkColumn> GetAllDatalinkColumns()
        {
            var columns = new Dictionary<long, DexihDatalinkColumn>();

            if (SourceDatalinkTable != null)
            {
                foreach (var column in SourceDatalinkTable.DexihDatalinkColumns)
                {
                    columns[column.DatalinkColumnKey] = column;
                }
            }

            foreach (var datalinkTransform in DexihDatalinkTransforms.OrderBy(c => c.Position))
            {
                // track any join columns for the transform.
                if (datalinkTransform.JoinDatalinkTable != null)
                {
                    foreach (var column in datalinkTransform.JoinDatalinkTable.DexihDatalinkColumns)
                    {
                        if(!columns.ContainsKey(column.DatalinkColumnKey))
                        {
                            columns[column.DatalinkColumnKey] = column;
                        }
                    }
                }

                // for any source mappings for the transform, copy the tracked instance.
                foreach (var item in datalinkTransform.DexihDatalinkTransformItems)
                {
                    if (item.SourceDatalinkColumn != null && !columns.ContainsKey(item.SourceDatalinkColumn.DatalinkColumnKey))
                    {
                        columns[item.SourceDatalinkColumn.DatalinkColumnKey] = item.SourceDatalinkColumn;
                    }

                    if (item.JoinDatalinkColumn != null && !columns.ContainsKey(item.JoinDatalinkColumn.DatalinkColumnKey))
                    {
                        columns[item.JoinDatalinkColumn.DatalinkColumnKey] = item.JoinDatalinkColumn;
                    }

                    if (item.TargetDatalinkColumn != null && !columns.ContainsKey(item.TargetDatalinkColumn.DatalinkColumnKey))
                    {
                        columns[item.TargetDatalinkColumn.DatalinkColumnKey] = item.TargetDatalinkColumn;
                    }

                    foreach (var param in item.DexihFunctionParameters)
                    {
                        if (param.DatalinkColumn != null && !columns.ContainsKey(param.DatalinkColumn.DatalinkColumnKey))
                        {
                            columns[param.DatalinkColumn.DatalinkColumnKey] = param.DatalinkColumn;
                        }

                        foreach (var paramArray in param.ArrayParameters.Where(c => c.DatalinkColumn != null))
                        {
                            if (!columns.ContainsKey(paramArray.DatalinkColumn.DatalinkColumnKey))
                            {
                                columns[paramArray.DatalinkColumn.DatalinkColumnKey] = paramArray.DatalinkColumn;
                            }
                        }
                    }
                }
            }

            return columns;
        }

        /// <summary>
        /// Resets all DatalinkColumns with the same reference/instance.
        /// </summary>
        public void ResetDatalinkColumns(Dictionary<long, DexihDatalinkColumn> columns = null)
        {
            if(columns == null)
            {
                columns = GetAllDatalinkColumns();
            }

            var newColumns = new HashSet<DexihDatalinkColumn>();

            if (SourceDatalinkTable != null)
            {
                foreach (var column in SourceDatalinkTable.DexihDatalinkColumns)
                {
                    newColumns.Add(columns.ContainsKey(column.GetPreservedColumnKey())
                        ? columns[column.GetPreservedColumnKey()]
                        : column);
                }
                SourceDatalinkTable.DexihDatalinkColumns = newColumns;
            }


            foreach (var datalinkTransform in DexihDatalinkTransforms.OrderBy(c=>c.Position))
            {
                foreach(var item in datalinkTransform.DexihDatalinkTransformItems)
                {
                    // track any join columns for the transform.
                    if (datalinkTransform.JoinDatalinkTable != null)
                    {
                        newColumns = new HashSet<DexihDatalinkColumn>();
                        foreach (var column in datalinkTransform.JoinDatalinkTable.DexihDatalinkColumns)
                        {
                            if (columns.ContainsKey(column.GetPreservedColumnKey()))
                            {
                                newColumns.Add(columns[column.GetPreservedColumnKey()]);
                            }
                            else
                            {
                                newColumns.Add(column);
                            }
                        }
                        datalinkTransform.JoinDatalinkTable.DexihDatalinkColumns = newColumns;
                    }
                    
                    if (item.FilterDatalinkColumn != null && columns.ContainsKey(item.FilterDatalinkColumn.GetPreservedColumnKey()))
                    {
                        item.FilterDatalinkColumnKey = item.FilterDatalinkColumn.GetPreservedColumnKey() > 0 ? item.FilterDatalinkColumn.GetPreservedColumnKey() : 0;
                        item.FilterDatalinkColumn = columns[item.FilterDatalinkColumn.GetPreservedColumnKey()];
                    }

                    if (item.SourceDatalinkColumn != null && columns.ContainsKey(item.SourceDatalinkColumn.GetPreservedColumnKey()))
                    {
                        item.SourceDatalinkColumnKey = item.SourceDatalinkColumn.GetPreservedColumnKey() > 0 ? item.SourceDatalinkColumn.GetPreservedColumnKey() : 0;
                        item.SourceDatalinkColumn = columns[item.SourceDatalinkColumn.GetPreservedColumnKey()];
                    }

                    if (item.JoinDatalinkColumn != null && columns.ContainsKey(item.JoinDatalinkColumn.GetPreservedColumnKey()))
                    {
                        item.JoinDatalinkColumnKey = item.JoinDatalinkColumn.GetPreservedColumnKey() > 0 ? item.JoinDatalinkColumn.GetPreservedColumnKey() : 0;
                        item.JoinDatalinkColumn = columns[item.JoinDatalinkColumn.GetPreservedColumnKey()];
                    }

                    if (item.TargetDatalinkColumn != null && columns.ContainsKey(item.TargetDatalinkColumn.GetPreservedColumnKey()))
                    {
                        item.TargetDatalinkColumnKey = item.TargetDatalinkColumn.GetPreservedColumnKey() > 0 ? item.TargetDatalinkColumn.GetPreservedColumnKey() : 0;
                        item.TargetDatalinkColumn = columns[item.TargetDatalinkColumn.GetPreservedColumnKey()];
                    }

					foreach (var param in item.DexihFunctionParameters)
                    {
                        if (param.DatalinkColumn != null && columns.ContainsKey(param.DatalinkColumn.GetPreservedColumnKey()))
                        {
                            param.DatalinkColumnKey = param.DatalinkColumn.GetPreservedColumnKey() > 0
                                ? param.DatalinkColumn.GetPreservedColumnKey()
                                : 0;
                            param.DatalinkColumn = columns[param.DatalinkColumn.GetPreservedColumnKey()];
                        }

                        foreach (var paramArray in param.ArrayParameters.Where(c => c.DatalinkColumn != null && columns.ContainsKey(c.DatalinkColumn.GetPreservedColumnKey())))
                        {
                            paramArray.DatalinkColumnKey = paramArray.DatalinkColumn.GetPreservedColumnKey() > 0 ? paramArray.DatalinkColumn.GetPreservedColumnKey() : 0;
                            paramArray.DatalinkColumn = columns[paramArray.DatalinkColumn.GetPreservedColumnKey()];
                        }
                    }
                }
            }

            //reset all new key values to 0
            foreach (var column in columns.Values.Where(c => c.DatalinkColumnKey < 0))
            {
                column.DatalinkColumnKey = 0;
                column.DatalinkTableKey = null;
            }
        }

        /// <summary>
        /// Retrieves all source tables for the datalink, including from dependent datalink, and joins.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<DexihTable> GetAllSourceTables(DexihHub hub, HashSet<long> ignoreDatalinks = null)
        {
            var tables = new Dictionary<long, DexihTable>();

            // check previously used datalinks to avoid infinite recursion.
            if (ignoreDatalinks == null)
            {
                ignoreDatalinks = new HashSet<long>();
            }
            
            if (ignoreDatalinks.Contains(DatalinkKey))
            {
                return tables.Values;
            }

            ignoreDatalinks.Add(DatalinkKey);

            if (SourceDatalinkTable?.SourceTableKey != null)
            {
                var table = hub.GetTableFromKey(SourceDatalinkTable.SourceTableKey.Value);
                if (table != null)
                {
                    tables.Add(table.TableKey, table);
                }
            }

            if (SourceDatalinkTable?.SourceDatalinkKey != null)
            {
                var datalink = hub.DexihDatalinks.SingleOrDefault(d => d.DatalinkKey == SourceDatalinkTable.SourceDatalinkKey);

                if (datalink != null)
                {
                    var sourceTables = datalink.GetAllSourceTables(hub);

                    foreach (var table in sourceTables)
                    {
                        if (!tables.ContainsKey(table.TableKey))
                        {
                            tables.Add(table.TableKey, table);
                        }
                    }

                }
            }
            
            foreach (var transform in DexihDatalinkTransforms)
            {
                if (transform.JoinDatalinkTable?.SourceTableKey != null)
                {
                    var table = hub.GetTableFromKey(transform.JoinDatalinkTable.SourceTableKey.Value);
                    if (table != null && !tables.ContainsKey(table.TableKey))
                    {
                        tables.Add(table.TableKey, table);
                    }
                }

                if (transform.JoinDatalinkTable?.SourceDatalinkKey != null)
                {
                    var datalink = hub.DexihDatalinks.SingleOrDefault(d => d.DatalinkKey == transform.JoinDatalinkTable.SourceDatalinkKey);

                    if (datalink != null)
                    {
                        var sourceTables = datalink.GetAllSourceTables(hub);

                        foreach (var table in sourceTables)
                        {
                            if (!tables.ContainsKey(table.TableKey))
                            {
                                tables.Add(table.TableKey, table);
                            }
                        }

                    }
                }
            }

            return tables.Values;
        }
    }
}

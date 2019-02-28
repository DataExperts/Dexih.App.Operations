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
            DexihDatalinkTargets = new HashSet<DexihDatalinkTarget>();
            EntityStatus = new EntityStatus();
        }

		[CopyCollectionKey((long)0, true)]
		public long DatalinkKey { get; set; }

        public long HubKey { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public long SourceDatalinkTableKey { get; set; }
        // public long? TargetTableKey { get; set; }
        public long? AuditConnectionKey { get; set; }

        public TransformDelta.EUpdateStrategy UpdateStrategy { get; set; } = TransformDelta.EUpdateStrategy.Reload;
        
        public TransformWriterTarget.ETransformWriterMethod LoadStrategy { get; set; }


        public EDatalinkType DatalinkType { get; set; }

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
        public ICollection<DexihDatalinkProfile> DexihDatalinkProfiles { get; set; }
        public ICollection<DexihDatalinkTransform> DexihDatalinkTransforms { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkTable> DexihDatalinkTables { get; set; }
        
        public ICollection<DexihDatalinkTarget> DexihDatalinkTargets { get; set; }


        /// <summary>
        /// Reference to the source columns for the datalink.
        /// </summary>
        public DexihDatalinkTable SourceDatalinkTable { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkStep> DexihDatalinkSteps { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public DexihHub Hub { get; set; }

        //[JsonIgnore, CopyIgnore]
        // public DexihTable TargetTable { get; set; }
        
	    [JsonIgnore, CopyReference]
        public DexihConnection AuditConnection { get; set; }


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
        /// Gets a single (deduplicated) list of all DatalinkColumns use by the datalink, and sets the instances to null
        /// </summary>
        /// <returns></returns>
        public Dictionary<long, DexihDatalinkColumn> GetAllDatalinkColumns()
        {
            var columns = new Dictionary<long, DexihDatalinkColumn>();

            if (SourceDatalinkTable != null)
            {
                foreach (var column in SourceDatalinkTable.DexihDatalinkColumns)
                {
                    AddColumns(column, columns);
                }
            }

            foreach (var datalinkTransform in DexihDatalinkTransforms.OrderBy(c => c.Position))
            {
                // track any join columns for the transform.
                if (datalinkTransform.JoinDatalinkTable != null)
                {
                    foreach (var column in datalinkTransform.JoinDatalinkTable.DexihDatalinkColumns)
                    {
                        AddColumns(column, columns);
                    }
                }

                datalinkTransform.NodeDatalinkColumnKey = datalinkTransform.NodeDatalinkColumn?.DatalinkColumnKey;
                AddColumns(datalinkTransform.NodeDatalinkColumn, columns);
                datalinkTransform.NodeDatalinkColumn = null;

                // for any source mappings for the transform, copy the tracked instance.
                foreach (var item in datalinkTransform.DexihDatalinkTransformItems)
                {
                    item.SourceDatalinkColumnKey = item.SourceDatalinkColumn?.DatalinkColumnKey;
                    AddColumns(item.SourceDatalinkColumn, columns);
                    item.SourceDatalinkColumn = null;

                    item.JoinDatalinkColumnKey = item.JoinDatalinkColumn?.DatalinkColumnKey;
                    AddColumns(item.JoinDatalinkColumn, columns);
                    item.JoinDatalinkColumn = null;

                    item.TargetDatalinkColumnKey = item.TargetDatalinkColumn?.DatalinkColumnKey;
                    AddColumns(item.TargetDatalinkColumn, columns);
                    item.TargetDatalinkColumn = null;

                    foreach (var param in item.DexihFunctionParameters)
                    {
                        param.DatalinkColumnKey = param.DatalinkColumn?.DatalinkColumnKey;
                        AddColumns(param.DatalinkColumn, columns);
                        param.DatalinkColumn = null;

                        foreach (var paramArray in param.ArrayParameters.Where(c => c.DatalinkColumn != null))
                        {
                            paramArray.DatalinkColumnKey = paramArray.DatalinkColumn?.DatalinkColumnKey;
                            AddColumns(paramArray.DatalinkColumn, columns);
                            paramArray.DatalinkColumn = null;
                        }
                    }
                }
            }

            return columns;
        }

        /// <summary>
        /// Adds the column and child columns into the columns collection.
        /// </summary>
        /// <param name="column"></param>
        /// <param name="columns"></param>
        private void AddColumns(DexihDatalinkColumn column, IDictionary<long, DexihDatalinkColumn> columns)
        {
            if (column != null && !columns.ContainsKey(column.DatalinkColumnKey))
            {
                columns[column.DatalinkColumnKey] = column;

                foreach (var childColumn in column.ChildColumns)
                {
                    AddColumns(childColumn, columns);
                }
            }
        }

        
        /// <summary>
        /// Resets the childcolumns of a column with unique column instances.
        /// </summary>
        /// <param name="column"></param>
        /// <param name="columns"></param>
        private void ResetChildColumns(DexihDatalinkColumn column, IReadOnlyDictionary<long, DexihDatalinkColumn> columns)
        {
            if (column.ChildColumns != null && column.ChildColumns.Any())
            {
                var childColumns = new HashSet<DexihDatalinkColumn>();
                foreach (var childColumn in column.ChildColumns)
                {
                    if (columns.ContainsKey(childColumn.DatalinkColumnKey))
                    {
                        childColumns.Add(columns[childColumn.DatalinkColumnKey]);
                    }
                    else
                    {
                        childColumns.Add(childColumn);
                    }
                    ResetChildColumns(childColumn, columns);
                }
                column.ChildColumns = childColumns;
            }
        }

        /// <summary>
        /// Resets all DatalinkColumns with the same reference/instance.
        /// Sets all negative datalink columns keys to 0
        /// </summary>
        public void ResetDatalinkColumns(Dictionary<long, DexihDatalinkColumn> columns = null)
        {
            if(columns == null)
            {
                columns = GetAllDatalinkColumns();
            }

            var newColumns = new HashSet<DexihDatalinkColumn>();
            
            //reset any child columns
            foreach (var column in columns.Values)
            {
                ResetChildColumns(column, columns);
            }
            
            if (SourceDatalinkTable != null)
            {
                foreach (var column in SourceDatalinkTable.DexihDatalinkColumns)
                {
                    newColumns.Add(columns.ContainsKey(column.DatalinkColumnKey)
                        ? columns[column.DatalinkColumnKey]
                        : column);
                }
                SourceDatalinkTable.DexihDatalinkColumns = newColumns;
            }

            foreach (var datalinkTransform in DexihDatalinkTransforms.OrderBy(c=>c.Position))
            {
                if (datalinkTransform.NodeDatalinkColumnKey != null &&
                    columns.ContainsKey(datalinkTransform.NodeDatalinkColumnKey.Value))
                {
                    datalinkTransform.NodeDatalinkColumn = columns[datalinkTransform.NodeDatalinkColumnKey.Value];
                    if (datalinkTransform.NodeDatalinkColumnKey < 0) datalinkTransform.NodeDatalinkColumnKey = 0;
                }
                
                if (datalinkTransform.NodeDatalinkColumnKey != null && columns.ContainsKey(datalinkTransform.NodeDatalinkColumnKey.Value))
                {
                    datalinkTransform.NodeDatalinkColumn = columns[datalinkTransform.NodeDatalinkColumnKey.Value];
                    if(datalinkTransform.NodeDatalinkColumnKey < 0) datalinkTransform.NodeDatalinkColumnKey = 0;
                }
                
                foreach(var item in datalinkTransform.DexihDatalinkTransformItems)
                {
                    // track any join columns for the transform.
                    if (datalinkTransform.JoinDatalinkTable != null)
                    {
                        newColumns = new HashSet<DexihDatalinkColumn>();
                        foreach (var column in datalinkTransform.JoinDatalinkTable.DexihDatalinkColumns)
                        {
                            if (columns.ContainsKey(column.DatalinkColumnKey))
                            {
                                newColumns.Add(columns[column.DatalinkColumnKey]);
                            }
                            else
                            {
                                newColumns.Add(column);
                            }
                        }
                        datalinkTransform.JoinDatalinkTable.DexihDatalinkColumns = newColumns;
                    }
                    
                    if (item.FilterDatalinkColumnKey != null && columns.ContainsKey(item.FilterDatalinkColumnKey.Value))
                    {
                        item.FilterDatalinkColumn = columns[item.FilterDatalinkColumnKey.Value];
                        if(item.FilterDatalinkColumnKey < 0) item.FilterDatalinkColumnKey = 0;
                    }

                    if (item.SourceDatalinkColumnKey != null && columns.ContainsKey(item.SourceDatalinkColumnKey.Value))
                    {
                        item.SourceDatalinkColumn = columns[item.SourceDatalinkColumnKey.Value];
                        if(item.SourceDatalinkColumnKey < 0) item.SourceDatalinkColumnKey = 0;
                    }

                    if (item.JoinDatalinkColumnKey != null && columns.ContainsKey(item.JoinDatalinkColumnKey.Value))
                    {
                        item.JoinDatalinkColumn = columns[item.JoinDatalinkColumnKey.Value];
                        if(item.JoinDatalinkColumnKey < 0) item.JoinDatalinkColumnKey = 0;
                    }

                    if (item.TargetDatalinkColumnKey != null && columns.ContainsKey(item.TargetDatalinkColumnKey.Value))
                    {
                        item.TargetDatalinkColumn = columns[item.TargetDatalinkColumnKey.Value];
                        if(item.TargetDatalinkColumnKey < 0) item.TargetDatalinkColumnKey = 0;
                    }

					foreach (var param in item.DexihFunctionParameters)
                    {
                        if (param.DatalinkColumnKey != null && columns.ContainsKey(param.DatalinkColumnKey.Value))
                        {
                            param.DatalinkColumn = columns[param.DatalinkColumnKey.Value];
                            if(param.DatalinkColumnKey < 0) param.DatalinkColumnKey = 0;
                        }

                        foreach (var paramArray in param.ArrayParameters.Where(c => c.DatalinkColumnKey != null && columns.ContainsKey(c.DatalinkColumnKey.Value)))
                        {
                            paramArray.DatalinkColumn = columns[paramArray.DatalinkColumnKey.Value];
                            if(paramArray.DatalinkColumnKey < 0) paramArray.DatalinkColumnKey = 0;
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

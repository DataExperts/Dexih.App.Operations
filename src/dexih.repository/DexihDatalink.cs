using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations.Schema;
using Newtonsoft.Json.Converters;
using Dexih.Utils.CopyProperties;
using System.Linq;
using dexih.functions.Query;
using dexih.transforms;
using dexih.transforms.Transforms;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public partial class DexihDatalink : DexihHubNamedEntity
    {




        public DexihDatalink()
        {
            DexihDatalinkProfiles = new HashSet<DexihDatalinkProfile>();
            DexihDatalinkSteps = new HashSet<DexihDatalinkStep>();
            DexihViews = new HashSet<DexihView>();
            DexihDatalinkTransforms = new HashSet<DexihDatalinkTransform>();
            DexihDatalinkTargets = new HashSet<DexihDatalinkTarget>();
            Parameters = new HashSet<DexihDatalinkParameter>();
            EntityStatus = new EntityStatus();
        }

        [ProtoMember(1)]
        public long SourceDatalinkTableKey { get; set; }

        [ProtoMember(2)]
        public long? AuditConnectionKey { get; set; }

        [ProtoMember(3)]
        public TransformDelta.EUpdateStrategy UpdateStrategy { get; set; } = TransformDelta.EUpdateStrategy.Reload;

        [ProtoMember(4)]
        public TransformWriterTarget.ETransformWriterMethod LoadStrategy { get; set; }

        [ProtoMember(5)]
        public EDatalinkType DatalinkType { get; set; }

        [ProtoMember(6)]
        public int RowsPerCommit { get; set; }

        [ProtoMember(7)]
        public int RowsPerProgress { get; set; }

        [ProtoMember(8)]
        public bool RollbackOnFail { get; set; }

        [ProtoMember(9)]
        public bool IsQuery { get; set; }

        [ProtoMember(10)]
        public int MaxRows { get; set; }

        [ProtoMember(11)]
        public bool AddDefaultRow { get; set; }

        [ProtoMember(12)]
        public string ProfileTableName { get; set; }

        [ProtoMember(13)]
        public bool IsShared { get; set; }

        [ProtoMember(14)]
        [NotMapped]
        public EntityStatus EntityStatus { get; set; }

        [ProtoMember(15)]
        public ICollection<DexihDatalinkProfile> DexihDatalinkProfiles { get; set; }

        [ProtoMember(16)]
        public ICollection<DexihDatalinkTransform> DexihDatalinkTransforms { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkTable> DexihDatalinkTables { get; set; }

        [ProtoMember(17)]
        public ICollection<DexihDatalinkTarget> DexihDatalinkTargets { get; set; }

        [ProtoMember(18)]
        public ICollection<DexihDatalinkParameter> Parameters { get; set; }


        /// <summary>
        /// Reference to the source columns for the datalink.
        /// </summary>
        [ProtoMember(19)]
        public DexihDatalinkTable SourceDatalinkTable { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkStep> DexihDatalinkSteps { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public ICollection<DexihView> DexihViews { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public DexihHub Hub { get; set; }
        
	    [JsonIgnore, CopyReference]
        public DexihConnection AuditConnection { get; set; }

        public void UpdateParameters(InputParameters inputParameters)
        {
            if (inputParameters == null || inputParameters.Count == 0 || Parameters == null || Parameters.Count == 0)
            {
                return;
            }

            foreach (var parameter in Parameters)
            {
                var inputParameter = inputParameters.SingleOrDefault(c => c.Name == parameter.Name);
                if (inputParameter != null)
                {
                    parameter.Value = inputParameter.Value;
                }
            }
        }

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
                if (transform.TransformType == ETransformType.Join)
                {
                    inputTables.Add(transform.JoinDatalinkTable);
                }

                // if the transform is a concatenate, then merge common column names together.
                if (transform.TransformType == ETransformType.Concatenate)
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

                datalinkTransform.NodeDatalinkColumnKey = datalinkTransform.NodeDatalinkColumn?.Key;
                AddColumns(datalinkTransform.NodeDatalinkColumn, columns);
                datalinkTransform.NodeDatalinkColumn = null;

                // for any source mappings for the transform, copy the tracked instance.
                foreach (var item in datalinkTransform.DexihDatalinkTransformItems)
                {
                    item.SourceDatalinkColumnKey = item.SourceDatalinkColumn?.Key;
                    AddColumns(item.SourceDatalinkColumn, columns);
                    item.SourceDatalinkColumn = null;

                    item.JoinDatalinkColumnKey = item.JoinDatalinkColumn?.Key;
                    AddColumns(item.JoinDatalinkColumn, columns);
                    item.JoinDatalinkColumn = null;

                    item.TargetDatalinkColumnKey = item.TargetDatalinkColumn?.Key;
                    AddColumns(item.TargetDatalinkColumn, columns);
                    item.TargetDatalinkColumn = null;

                    item.FilterDatalinkColumnKey = item.FilterDatalinkColumn?.Key;
                    AddColumns(item.FilterDatalinkColumn, columns);
                    item.FilterDatalinkColumn = null;

                    foreach (var param in item.DexihFunctionParameters)
                    {
                        param.DatalinkColumnKey = param.DatalinkColumn?.Key;
                        AddColumns(param.DatalinkColumn, columns);
                        param.DatalinkColumn = null;

                        foreach (var paramArray in param.ArrayParameters.Where(c => c.DatalinkColumn != null))
                        {
                            paramArray.DatalinkColumnKey = paramArray.DatalinkColumn?.Key;
                            AddColumns(paramArray.DatalinkColumn, columns);
                            paramArray.DatalinkColumn = null;
                        }
                    }
                }
            }

            foreach (var target in DexihDatalinkTargets)
            {
                if (target.NodeDatalinkColumn != null)
                {
                    target.NodeDatalinkColumnKey = target.NodeDatalinkColumn?.Key;
                    AddColumns(target.NodeDatalinkColumn, columns);
                    target.NodeDatalinkColumn = null;
                }
            }

            return columns;
        }

        /// <summary>
        /// Adds the column and child columns into the columns collection.
        /// </summary>
        /// <param name="column"></param>
        /// <param name="columns"></param>
        private void AddColumns(DexihDatalinkColumn column, IDictionary<long, DexihDatalinkColumn> columns, long? parentColumnKey = null)
        {
            if (column != null && !columns.ContainsKey(column.Key))
            {
                if (column.ParentDatalinkColumnKey == null)
                {
                    column.ParentDatalinkColumnKey = parentColumnKey;    
                }
                
                columns[column.Key] = column;

                foreach (var childColumn in column.ChildColumns)
                {
                    AddColumns(childColumn, columns, column.Key);
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
                    if (columns.ContainsKey(childColumn.Key))
                    {
                        childColumns.Add(columns[childColumn.Key]);
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
                    newColumns.Add(columns.ContainsKey(column.Key)
                        ? columns[column.Key]
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

                // track any join columns for the transform.
                if (datalinkTransform.JoinDatalinkTable != null)
                {
                    newColumns = new HashSet<DexihDatalinkColumn>();
                    foreach (var column in datalinkTransform.JoinDatalinkTable.DexihDatalinkColumns)
                    {
                        if (columns.ContainsKey(column.Key))
                        {
                            newColumns.Add(columns[column.Key]);
                        }
                        else
                        {
                            newColumns.Add(column);
                        }
                    }
                    datalinkTransform.JoinDatalinkTable.DexihDatalinkColumns = newColumns;
                }
                
                foreach(var item in datalinkTransform.DexihDatalinkTransformItems)
                {
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

            foreach (var target in DexihDatalinkTargets)
            {
                if (target.NodeDatalinkColumnKey != null &&
                    columns.ContainsKey(target.NodeDatalinkColumnKey.Value))
                {
                    target.NodeDatalinkColumn = columns[target.NodeDatalinkColumnKey.Value];
                    if (target.NodeDatalinkColumnKey < 0) target.NodeDatalinkColumnKey = 0;
                }
                
                if (target.NodeDatalinkColumnKey != null && columns.ContainsKey(target.NodeDatalinkColumnKey.Value))
                {
                    target.NodeDatalinkColumn = columns[target.NodeDatalinkColumnKey.Value];
                    if(target.NodeDatalinkColumnKey < 0) target.NodeDatalinkColumnKey = 0;
                }
            }

            //reset all new key values to 0
            foreach (var column in columns.Values.Where(c => c.Key < 0))
            {
                column.Key = 0;
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
            
            if (ignoreDatalinks.Contains(Key))
            {
                return tables.Values;
            }

            ignoreDatalinks.Add(Key);

            if (SourceDatalinkTable?.SourceTableKey != null)
            {
                var table = hub.GetTableFromKey(SourceDatalinkTable.SourceTableKey.Value);
                if (table != null)
                {
                    tables.Add(table.Key, table);
                }
            }

            if (SourceDatalinkTable?.SourceDatalinkKey != null)
            {
                var datalink = hub.DexihDatalinks.SingleOrDefault(d => d.Key == SourceDatalinkTable.SourceDatalinkKey);

                if (datalink != null)
                {
                    var sourceTables = datalink.GetAllSourceTables(hub);

                    foreach (var table in sourceTables)
                    {
                        if (!tables.ContainsKey(table.Key))
                        {
                            tables.Add(table.Key, table);
                        }
                    }

                }
            }
            
            foreach (var transform in DexihDatalinkTransforms)
            {
                if (transform.JoinDatalinkTable?.SourceTableKey != null)
                {
                    var table = hub.GetTableFromKey(transform.JoinDatalinkTable.SourceTableKey.Value);
                    if (table != null && !tables.ContainsKey(table.Key))
                    {
                        tables.Add(table.Key, table);
                    }
                }

                if (transform.JoinDatalinkTable?.SourceDatalinkKey != null)
                {
                    var datalink = hub.DexihDatalinks.SingleOrDefault(d => d.Key == transform.JoinDatalinkTable.SourceDatalinkKey);

                    if (datalink != null)
                    {
                        var sourceTables = datalink.GetAllSourceTables(hub);

                        foreach (var table in sourceTables)
                        {
                            if (!tables.ContainsKey(table.Key))
                            {
                                tables.Add(table.Key, table);
                            }
                        }

                    }
                }
            }

            return tables.Values;
        }
    }
}

using System;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Linq;

namespace dexih.repository
{
    public class DexihDatalinkTable: DexihBaseEntity
    {


        public DexihDatalinkTable()
        {
            DexihDatalinkSourceTables = new HashSet<DexihDatalink>();
            DexihDatalinkColumns = new HashSet<DexihDatalinkColumn>();
            DexihDatalinkTransforms = new HashSet<DexihDatalinkTransform>();
        }

        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }

		[CopyCollectionKey()]
        public long DatalinkTableKey { get; set; }

	    [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
	    public long? SourceTableKey { get; set; }

	    [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
	    public long? SourceDatalinkKey { get; set; }
	    
	    public int? RowsStartAt { get; set; }
	    public int? RowsEndAt { get; set; }
	    public int? RowsIncrement { get; set; }
	    
        public string Name { get; set; }

        public ESourceType SourceType { get; set; }

        public ICollection<DexihDatalinkColumn> DexihDatalinkColumns { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihTable SourceTable { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDatalink SourceDatalink { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalink> DexihDatalinkSourceTables { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkTransform> DexihDatalinkTransforms { get; set; }

        /// <summary>
        /// Converts the datalinkTable to a base "Table" class.
        /// </summary>
        /// <returns></returns>
		public Table GetTable(Table sourceTable, InputColumn[] inputColumns)
        { 
			Table table;

	        if (sourceTable == null)
	        {
		        table = new Table();
		        this.CopyProperties(table, false);
	        }
	        else
	        {
		        table = (Table) sourceTable.CloneProperties(false);
	        }

            foreach (var dbColumn in DexihDatalinkColumns.Where(c => c.IsValid).OrderBy(c => c.Position))
            {
				table.Columns.Add(dbColumn.GetTableColumn(inputColumns));
            }

            return table;
        }

        /// <summary>
        /// Gets an array containing all the parent-child nodes to a particular column.
        /// </summary>
        /// <param name="columnKey"></param>
        /// <returns></returns>
        public List<DexihDatalinkColumn> GetNodePath(long columnKey, ICollection<DexihDatalinkColumn> childColumns = null)
        {
	        var columns = childColumns ?? DexihDatalinkColumns;
	        if (columns != null)
	        {
		        foreach (var column in columns)
		        {
			        if (columnKey == column.DatalinkColumnKey)
			        {
				        return new List<DexihDatalinkColumn> {column};
			        }

			        if (column.ChildColumns != null)
			        {
				        var path = GetNodePath(columnKey, column.ChildColumns);
				        if (path != null)
				        {
					        path.Insert(0, column);
					        return path;
				        }
			        }
		        }
	        }

	        return null;
        }
    }
}

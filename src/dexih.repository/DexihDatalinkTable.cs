using dexih.functions;
using Dexih.Utils.CopyProperties;

using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;


namespace dexih.repository
{
	[DataContract]
    public class DexihDatalinkTable: DexihHubNamedEntity
    {


        public DexihDatalinkTable()
        {
            DexihDatalinkSourceTables = new HashSet<DexihDatalink>();
            DexihDatalinkColumns = new HashSet<DexihDatalinkColumn>();
            DexihDatalinkTransforms = new HashSet<DexihDatalinkTransform>();
        }



        [DataMember(Order = 7)]
        // [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
	    public long? SourceTableKey { get; set; }

        [DataMember(Order = 8)]
        // [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
	    public long? SourceDatalinkKey { get; set; }

        [DataMember(Order = 9)]
        public int? RowsStartAt { get; set; } = 1;

        [DataMember(Order = 10)]
        public int? RowsEndAt { get; set; } = 1;

        [DataMember(Order = 11)]
        public int? RowsIncrement { get; set; } = 1;

        [DataMember(Order = 12)] 
        public ESourceType SourceType { get; set; } = ESourceType.Table;

        [DataMember(Order = 13)]
        public bool DisablePushDown { get; set; }
        
        [DataMember(Order = 14)]
        public ICollection<DexihDatalinkColumn> DexihDatalinkColumns { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihTable SourceTable { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihDatalink SourceDatalink { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public ICollection<DexihDatalink> DexihDatalinkSourceTables { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public ICollection<DexihDatalinkTransform> DexihDatalinkTransforms { get; set; }

        public override void ResetKeys()
        {
	        Key = 0;
            
	        foreach (var column in DexihDatalinkColumns)
	        {
		        column.ResetKeys();
	        }
        }

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
        /// <param name="childColumns"></param>
        /// <returns></returns>
        public List<DexihDatalinkColumn> GetNodePath(long columnKey, ICollection<DexihDatalinkColumn> childColumns = null)
        {
	        var columns = childColumns ?? DexihDatalinkColumns;
	        if (columns != null)
	        {
		        foreach (var column in columns)
		        {
			        if (columnKey == column.Key)
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

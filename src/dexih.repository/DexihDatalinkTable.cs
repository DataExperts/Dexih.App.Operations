using dexih.functions;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;

namespace dexih.repository
{
    public class DexihDatalinkTable: DexihBaseEntity
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public enum ESourceType
        {
            Datalink,
            Table,
	        Rows,
            Function
        }

        public DexihDatalinkTable()
        {
            DexihDatalinkSourceTables = new HashSet<DexihDatalink>();
            DexihDatalinkColumns = new HashSet<DexihDatalinkColumn>();
            DexihDatalinkSourceTables = new HashSet<DexihDatalink>();
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

        public virtual ICollection<DexihDatalinkColumn> DexihDatalinkColumns { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihTable SourceTable { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihDatalink SourceDatalink { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual ICollection<DexihDatalink> DexihDatalinkSourceTables { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual ICollection<DexihDatalinkTransform> DexihDatalinkTransforms { get; set; }

        /// <summary>
        /// Converts the datalinkTable to a base "Table" class.
        /// </summary>
        /// <returns></returns>
		public Table GetTable(Table sourceTable, IEnumerable<DexihColumnBase> inputColumns)
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

    }
}

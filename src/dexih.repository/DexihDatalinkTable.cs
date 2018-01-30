using dexih.functions;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using static dexih.transforms.Connection;

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

        [NotMapped]
        public ESourceType SourceType { get; set; }

        [JsonIgnore, CopyIgnore]
        public string SourceTypeString
        {
            get => SourceType.ToString();
            set => SourceType = (ESourceType)Enum.Parse(typeof(ESourceType), value);
        }

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
		public Table GetTable(DexihTable sourceTable = null, ECategory databaseTypeCategory = ECategory.SqlDatabase)
        {
			Table table;
			switch (databaseTypeCategory)
			{
				case ECategory.File:
					table = new FlatFile();
					((FlatFile)table).FileConfiguration = sourceTable?.FileFormat?.GetFileFormat();
					break;
				case ECategory.WebService:
					table = new WebService();
					break;
				default:
					table = new Table();
					break;
			}

	        if (sourceTable == null)
	        {
		        this.CopyProperties(table, false);
	        }
	        else
	        {
		        sourceTable.CopyProperties(table, false);
	        }

            foreach (var dbColumn in DexihDatalinkColumns.Where(c => c.IsValid).OrderBy(c => c.Position))
            {
				table.Columns.Add(dbColumn.GetTableColumn());
            }

            return table;
        }

    }
}

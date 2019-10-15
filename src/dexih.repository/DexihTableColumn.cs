using System.Collections.Generic;

using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text.Json.Serialization;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using MessagePack;

namespace dexih.repository
{
	[MessagePackObject]
    public class DexihTableColumn : DexihColumnBase
    {
	    public DexihTableColumn()
	    {
		    ChildColumns = new HashSet<DexihTableColumn>();
	    }

        [Key(24)]
        [CopyParentCollectionKey(nameof(Key))]
		public long? TableKey { get; set; }
	    
	    [JsonIgnore, IgnoreMember, CopyParentCollectionKey(nameof(ParentColumnKey))]
	    public long? ParentColumnKey { get; set; }
	    
	    [JsonIgnore, CopyIgnore, IgnoreMember]
	    public DexihTableColumn ParentColumn { get; set; }

        [Key(25)]
        public ICollection<DexihTableColumn> ChildColumns { get; set; }

        [Key(26)]
        public long? ColumnValidationKey { get; set; }

        [Key(27)]
        [NotMapped]
        public EntityStatus EntityStatus { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihTable Table { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihColumnValidation ColumnValidation { get; set; }

	    [JsonIgnore, CopyIgnore, IgnoreMember]
	    public ICollection<DexihColumnValidation> DexihColumnValidationLookupColumn { get; set; }
	    
	    public TableColumn GetTableColumn(InputColumn[] inputColumns)
	    {
		    var tableColumn = new TableColumn();
		    this.CopyProperties(tableColumn, true);
		    tableColumn.ReferenceTable = TableKey.ToString();

		    if (ChildColumns != null && ChildColumns.Count > 0)
		    {
			    tableColumn.ChildColumns = new TableColumns();
			    foreach (var childColumn in ChildColumns.OrderBy(c => c.Position))
			    {
				    tableColumn.ChildColumns.Add(childColumn.GetTableColumn(inputColumns));
			    }
		    }
		    
		    var column = inputColumns?.SingleOrDefault(c => c.Name == tableColumn.Name);
		    if (column != null)
		    {
			    tableColumn.DefaultValue = column.Value;
		    }
		    return tableColumn;
	    }

	    public long GetParentTableKey()
	    {
		    return ParentColumnKey ?? ParentColumn.GetParentTableKey();
	    }
    }
}

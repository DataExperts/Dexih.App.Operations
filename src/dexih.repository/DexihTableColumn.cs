using System.Collections.Generic;
using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using dexih.functions;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    public partial class DexihTableColumn : DexihColumnBase
    {
	    public DexihTableColumn()
	    {
		    ChildColumns = new HashSet<DexihTableColumn>();
	    }
	    
        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }

        [CopyCollectionKey((long)0, true)]
        public long ColumnKey { get; set; }
		
	    [CopyParentCollectionKey(nameof(DexihTable.TableKey))]
		public long? TableKey { get; set; }
	    
	    [JsonIgnore, CopyParentCollectionKey(nameof(ParentColumnKey))]
	    public long? ParentColumnKey { get; set; }
	    
	    [JsonIgnore, CopyIgnore]
	    public DexihTableColumn ParentColumn { get; set; }
	    
	    public ICollection<DexihTableColumn> ChildColumns { get; set; }

	    public long? ColumnValidationKey { get; set; }

        [NotMapped]
        public EntityStatus EntityStatus { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihTable Table { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihColumnValidation ColumnValidation { get; set; }

	    [JsonIgnore, CopyIgnore]
	    public virtual ICollection<DexihColumnValidation> DexihColumnValidationLookupColumn { get; set; }
	    
	    public TableColumn GetTableColumn(InputColumn[] inputColumns)
	    {
		    var tableColumn = new TableColumn();
		    this.CopyProperties(tableColumn, false);
		    tableColumn.ReferenceTable = TableKey.ToString();

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

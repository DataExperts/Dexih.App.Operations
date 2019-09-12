using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using ProtoBuf;

namespace dexih.repository
{
	[ProtoContract]
    public class DexihTableColumn : DexihColumnBase
    {
	    public DexihTableColumn()
	    {
		    ChildColumns = new HashSet<DexihTableColumn>();
	    }

        [ProtoMember(1)]
        [CopyParentCollectionKey(nameof(Key))]
		public long? TableKey { get; set; }
	    
	    [JsonIgnore, CopyParentCollectionKey(nameof(ParentColumnKey))]
	    public long? ParentColumnKey { get; set; }
	    
	    [JsonIgnore, CopyIgnore]
	    public DexihTableColumn ParentColumn { get; set; }

        [ProtoMember(2)]
        public ICollection<DexihTableColumn> ChildColumns { get; set; }

        [ProtoMember(3)]
        public long? ColumnValidationKey { get; set; }

        [ProtoMember(4)]
        [NotMapped]
        public EntityStatus EntityStatus { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihTable Table { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihColumnValidation ColumnValidation { get; set; }

	    [JsonIgnore, CopyIgnore]
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

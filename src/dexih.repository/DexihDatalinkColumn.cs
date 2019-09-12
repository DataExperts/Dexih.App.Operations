using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using ProtoBuf;

namespace dexih.repository
{
	[ProtoContract]
    public partial class DexihDatalinkColumn : DexihColumnBase
    {
        public DexihDatalinkColumn()
        {
            DexihDatalinkTransformItemsSourceColumn = new HashSet<DexihDatalinkTransformItem>();
            DexihDatalinkTransformItemsTargetColumn = new HashSet<DexihDatalinkTransformItem>();
	        DexihDatalinkTransformItemsJoinColumn = new HashSet<DexihDatalinkTransformItem>();
            DexihFunctionParameterColumn = new HashSet<DexihFunctionParameter>();
            DexihDatalinkTransformsJoinSortColumn = new HashSet<DexihDatalinkTransform>();
            DexihDatalinkTransformsNodeColumn = new HashSet<DexihDatalinkTransform>();
	        ChildColumns = new HashSet<DexihDatalinkColumn>();
        }

        // don't reset negative keys here, as they need to be maintained when copying datalinks across.
        [ProtoMember(1)]
        [CopyCollectionKey((long)0, false)]
        public new long Key { get; set; }

        [ProtoMember(2)]
        [CopyParentCollectionKey(nameof(DexihDatalinkTable.Key), nameof(DexihDatalinkTable))]
        public long? DatalinkTableKey { get; set; }
	    
	    [JsonIgnore, CopyIgnore]
	    public DexihDatalinkColumn ParentColumn { get; set; }
	    
	    [JsonIgnore, CopyParentCollectionKey(nameof(DexihDatalinkColumn.Key), nameof(DexihDatalinkColumn))]
	    public long? ParentDatalinkColumnKey { get; set; }

        [ProtoMember(3)]
        public ICollection<DexihDatalinkColumn> ChildColumns { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDatalinkTable DatalinkTable { get; set; }


        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkTransformItem> DexihDatalinkTransformItemsSourceColumn { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkTransformItem> DexihDatalinkTransformItemsTargetColumn { get; set; }

	    [JsonIgnore, CopyIgnore]
	    public ICollection<DexihDatalinkTransformItem> DexihDatalinkTransformItemsJoinColumn { get; set; }

	    [JsonIgnore, CopyIgnore]
	    public ICollection<DexihDatalinkTransformItem> DexihDatalinkTransformItemsFilterColumn { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihFunctionParameter> DexihFunctionParameterColumn { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkTransform> DexihDatalinkTransformsJoinSortColumn { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkTransform> DexihDatalinkTransformsNodeColumn { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkTarget> DexihDatalinkTargetNodeColumn { get; set; }

		public TableColumn GetTableColumn(InputColumn[] inputColumns)
		{
			var tableColumn = new TableColumn();
			this.CopyProperties(tableColumn, true);

			if (ChildColumns != null && ChildColumns.Count > 0)
			{
				tableColumn.ChildColumns = new TableColumns();
				foreach (var childColumn in ChildColumns.OrderBy(c => c.Position))
				{
					tableColumn.ChildColumns.Add(childColumn.GetTableColumn(inputColumns));
				}
			}

			var topParent = this;
			if (topParent.ParentColumn != null) topParent = topParent.ParentColumn;
			
			tableColumn.ReferenceTable = topParent.DatalinkTableKey.ToString();

			var column = inputColumns?.SingleOrDefault(c => c.Name == tableColumn.Name);
			if (column != null)
			{
				tableColumn.DefaultValue = column.Value;
			}
			return tableColumn;
		}

    }
}

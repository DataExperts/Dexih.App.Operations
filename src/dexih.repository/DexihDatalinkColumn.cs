using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using dexih.functions;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
	[Serializable]
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
        [CopyCollectionKey((long)0, false)]
        public long Key { get; set; }

	    [CopyParentCollectionKey(nameof(DexihDatalinkTable.Key))]
        public long? DatalinkTableKey { get; set; }
	    
	    [JsonIgnore, CopyIgnore]
	    public DexihDatalinkColumn ParentColumn { get; set; }
	    
	    [JsonIgnore, CopyParentCollectionKey(nameof(ParentDatalinkColumnKey))]
	    public long? ParentDatalinkColumnKey { get; set; }
	    
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

using System.Collections.Generic;
using Newtonsoft.Json;
using dexih.functions;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    public partial class DexihDatalinkColumn : DexihColumnBase
    {
        public DexihDatalinkColumn()
        {
            DexihDatalinkTransformItemsSourceColumn = new HashSet<DexihDatalinkTransformItem>();
            DexihDatalinkTransformItemsTargetColumn = new HashSet<DexihDatalinkTransformItem>();
	        DexihDatalinkTransformItemsJoinColumn = new HashSet<DexihDatalinkTransformItem>();
            DexihFunctionParameterColumn = new HashSet<DexihFunctionParameter>();
            DexihDatalinkTransforms = new HashSet<DexihDatalinkTransform>();
        }

        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }

	    [CopyCollectionKey((long)0, true)]
        public long DatalinkColumnKey { get; set; }

        public long? DatalinkTableKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDatalinkTable DatalinkTable { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkTransformItem> DexihDatalinkTransformItemsSourceColumn { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkTransformItem> DexihDatalinkTransformItemsTargetColumn { get; set; }

	    [JsonIgnore, CopyIgnore]
	    public ICollection<DexihDatalinkTransformItem> DexihDatalinkTransformItemsJoinColumn { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihFunctionParameter> DexihFunctionParameterColumn { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkTransform> DexihDatalinkTransforms { get; set; }

		public TableColumn GetTableColumn()
		{
			var tableColumn = new TableColumn();
			this.CopyProperties(tableColumn, false);
			tableColumn.ReferenceTable = DatalinkTableKey.ToString();
			return tableColumn;
		}


    }
}

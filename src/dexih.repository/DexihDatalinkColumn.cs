using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Newtonsoft.Json;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using Microsoft.CodeAnalysis.CSharp.Syntax;

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

	    [CopyCollectionKey(0L, false)]
	    public long DatalinkColumnKey { get; set; }

	    [CopyParentCollectionKey]
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
	    public ICollection<DexihDatalinkTransformItem> DexihDatalinkTransformItemsFilterColumn { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihFunctionParameter> DexihFunctionParameterColumn { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkTransform> DexihDatalinkTransforms { get; set; }

		public TableColumn GetTableColumn(InputColumn[] inputColumns)
		{
			var tableColumn = new TableColumn();
			this.CopyProperties(tableColumn, false);
			tableColumn.ReferenceTable = DatalinkTableKey.ToString();

			var column = inputColumns?.SingleOrDefault(c => c.Name == tableColumn.Name);
			if (column != null)
			{
				tableColumn.DefaultValue = column.Value;
			}
			return tableColumn;
		}

//	    /// <summary>
//	    /// Used to keep an original datalink column key when reloading references.
//	    /// </summary>
//	    [NotMapped, JsonIgnore]
//	    public long OldDatalinkColumnKey { get; set; }
//
//	    public long GetPreservedColumnKey()
//	    {
//		    if (DatalinkColumnKey == 0)
//		    {
//			    return OldDatalinkColumnKey;
//		    }
//
//		    return DatalinkColumnKey;
//	    }


    }
}

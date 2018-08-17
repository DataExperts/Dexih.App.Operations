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

	    private long _datlainkColumnKey;
	    
	    [CopyCollectionKey((long)0, true)]
        public long DatalinkColumnKey {
		    get => _datlainkColumnKey;
		    set
		    {
			    if (value == 0)
			    {
				    var a = 1;
			    }
			    if (value != 0)
			    {
				    OldDatalinkColumnKey = value;
			    }

			    _datlainkColumnKey = value;
		    } 
	    }

	    // [CopyParentCollectionKey]
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

		public TableColumn GetTableColumn(IEnumerable<DexihColumnBase> inputColumns)
		{
			var tableColumn = new TableColumn();
			this.CopyProperties(tableColumn, false);
			tableColumn.ReferenceTable = DatalinkTableKey.ToString();

			if (inputColumns != null)
			{
				var inputColumn = inputColumns.SingleOrDefault(c => c.Name == tableColumn.Name);
				if (inputColumn != null)
				{
					tableColumn.DefaultValue = inputColumn.DefaultValue;
				}
			}
			return tableColumn;
		}

	    /// <summary>
	    /// Used to keep an original datalink column key when reloading references.
	    /// </summary>
	    [NotMapped, JsonIgnore]
	    public long OldDatalinkColumnKey { get; set; }

	    public long GetPreservedColumnKey()
	    {
		    if (DatalinkColumnKey == 0)
		    {
			    return OldDatalinkColumnKey;
		    }

		    return DatalinkColumnKey;
	    }


    }
}

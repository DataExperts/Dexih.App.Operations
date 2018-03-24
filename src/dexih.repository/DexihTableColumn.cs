﻿using System.Collections.Generic;
using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations.Schema;
using dexih.functions;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    public partial class DexihTableColumn : DexihColumnBase
    {
        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }

        [CopyCollectionKey((long)0, true)]
        public long ColumnKey { get; set; }
		
	    [CopyParentCollectionKey]
		public long TableKey { get; set; }
        
	    public long? ColumnValidationKey { get; set; }

        [NotMapped]
        public EntityStatus EntityStatus { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihTable Table { get; set; }

        public virtual DexihColumnValidation ColumnValidation { get; set; }

	    [JsonIgnore, CopyIgnore]
	    public virtual ICollection<DexihColumnValidation> DexihColumnValidationLookupColumn { get; set; }
	   
		public TableColumn GetTableColumn()
		{
			var tableColumn = new TableColumn();
			this.CopyProperties(tableColumn, false);
			tableColumn.ReferenceTable = TableKey.ToString();
			return tableColumn;
		}

    }
}

using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    [DataContract]
    public class DexihTableIndex: DexihHubNamedEntity
    {	    
        public DexihTableIndex()
        {
            Columns = new HashSet<DexihTableIndexColumn>();
        }

        [DataMember(Order = 7)]
        [CopyParentCollectionKey(nameof(Key))]
        public long TableKey { get; set; }
        
        [DataMember(Order = 8)]
        public ICollection<DexihTableIndexColumn> Columns { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihTable Table { get; set; }
    }
}
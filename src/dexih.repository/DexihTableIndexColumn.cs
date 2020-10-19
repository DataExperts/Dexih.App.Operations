using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using dexih.functions;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    [DataContract]
    public class DexihTableIndexColumn: DexihHubKeyEntity
    {
        [DataMember(Order = 5)]
        [CopyParentCollectionKey(nameof(Key))]
        public long? TableIndexKey { get; set; }
        
        [DataMember(Order = 6)]
        public long ColumnKey { get; set; }
        
        [DataMember(Order = 7)] 
        public ESortDirection Direction { get; set; } = ESortDirection.Ascending;
        
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihTableIndex TableIndex { get; set; }
    }
}
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    [DataContract]
    public class DexihTagObject: DexihHubEntity
    {
        [DataMember(Order = 4)]
        public long TagKey { get; set; }
        
        [DataMember(Order = 5)]
        public long ObjectKey { get; set; }
        
        [DataMember(Order = 6)]
        public ESharedObjectType ObjectType { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihTag DexihTag { get; set; }
    }
}
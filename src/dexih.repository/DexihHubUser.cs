using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public partial class DexihHubUser : DexihBaseEntity
    {


        [Key(3)]
        [JsonIgnore, CopyIgnore, IgnoreMember]
        public long HubKey { get; set; }

        [Key(4)]
        public string UserId { get; set; }

        [Key(5)]
        public EPermission Permission { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public virtual DexihHub Hub { get; set; }
    }
}

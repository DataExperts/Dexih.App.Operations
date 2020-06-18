using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;


namespace dexih.repository
{
    [DataContract]
    public class DexihHubUser : DexihBaseEntity
    {


        [DataMember(Order = 3)]
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public long HubKey { get; set; }

        [DataMember(Order = 4)]
        public string UserId { get; set; }

        [DataMember(Order = 5)]
        public EPermission Permission { get; set; }
        
        [DataMember(Order = 6)]
        public bool ReceiveAlerts { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public virtual DexihHub Hub { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using dexih.functions;
using Dexih.Utils.CopyProperties;


namespace dexih.repository
{
    [DataContract]
    public class DexihRemoteAgent : DexihBaseEntity
    {        
        public DexihRemoteAgent()
        {
            DexihRemoteAgentHubs = new HashSet<DexihRemoteAgentHub>();
        }
        
        [DataMember(Order = 3)]
        [CopyCollectionKey((long)0, true)]
        public long RemoteAgentKey { get; set; }

        [DataMember(Order = 4)]
        public string Name { get; set; }

        [DataMember(Order = 5)]
        public string UserId { get; set; }

        [DataMember(Order = 6)]
        public bool RestrictIp { get; set; }

        [DataMember(Order = 7)]
        public bool AllowExternalConnect { get; set; }
        
        [DataMember(Order = 8)]
        [NotMapped, CopyIgnore]
        public string[] IpAddresses
        {
            get
            {
                if (string.IsNullOrEmpty(IpAddressesString))
                {
                    return null;
                }
                else
                {
                    return IpAddressesString.Deserialize<string[]>(true);
                }
            }
            set
            {
                if (value == null)
                {
                    IpAddressesString = null;
                }
                else
                {
                    IpAddressesString = value.Serialize();
                }
            }
        }

        [JsonIgnore]
        public string IpAddressesString { get; set; }
        
        [DataMember(Order = 9)]
        public string RemoteAgentId { get; set; }

        [DataMember(Order = 10)]
        public string HashedToken { get; set; }

        [DataMember(Order = 11)]
        public string LastLoginIpAddress { get; set; }

        [DataMember(Order = 12)]
        public DateTime? LastLoginDateTime { get; set; }
        
        [DataMember(Order = 14)]
        public virtual ICollection<DexihRemoteAgentHub> DexihRemoteAgentHubs { get; set; }
    }
}

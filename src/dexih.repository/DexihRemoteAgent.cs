using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text.Json;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public partial class DexihRemoteAgent : DexihBaseEntity
    {        
        public DexihRemoteAgent()
        {
            DexihRemoteAgentHubs = new HashSet<DexihRemoteAgentHub>();
        }
        
        [Key(3)]
        [CopyCollectionKey((long)0, true)]
        public long RemoteAgentKey { get; set; }

        [Key(4)]
        public string Name { get; set; }

        [Key(5)]
        public string UserId { get; set; }

        [Key(6)]
        public bool RestrictIp { get; set; }

        [Key(7)]
        public bool AllowExternalConnect { get; set; }
        
        [Key(8)]
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
                    return JsonSerializer.Deserialize<string[]>(IpAddressesString);
                }
            }
            set
            {
                if (value == null)
                {
                    IpAddressesString = null;
                }
                else {
                    IpAddressesString = JsonSerializer.Serialize(value);
                }
            }
        }

        [JsonIgnore]
        public string IpAddressesString { get; set; }
        
        [Key(9)]
        public string RemoteAgentId { get; set; }

        [Key(10)]
        public string HashedToken { get; set; }

        [Key(11)]
        public string LastLoginIpAddress { get; set; }

        [Key(12)]
        public DateTime? LastLoginDateTime { get; set; }
        
        [Key(14)]
        public virtual ICollection<DexihRemoteAgentHub> DexihRemoteAgentHubs { get; set; }
    }
}

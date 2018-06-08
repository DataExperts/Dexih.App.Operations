using System;
using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using dexih.transforms;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.MessageHelpers;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace dexih.repository
{
    public partial class DexihRemoteAgent : DexihBaseEntity
    {
        [CopyCollectionKey((long)0, true)]
        public long RemoteAgentKey { get; set; }

        // [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }
        public string Name { get; set; }
        
        public bool RestrictIp { get; set; }

        [NotMapped]
        public string[] IpAddresses { get; set; }

        [JsonIgnore, CopyIgnore]
        public string IpAddressesString
        {
            get => IpAddresses == null ? null : string.Join(",", IpAddresses);
            set => IpAddresses = value?.Split(',').ToArray();
        }
        public string RemoteAgentId { get; set; }

        public bool IsDefault { get; set; }
        public bool IsAuthorized { get; set; }
        public bool AllowExternalConnect { get; set; }

        public string LastLoginIpAddress { get; set; }
        public DateTime? LastLoginDate { get; set; } 
        
        [JsonIgnore, CopyIgnore]
        public virtual DexihHub Hub { get; set; }

        [NotMapped]
        public DexihActiveAgent ActiveAgent { get; set; }

    }
}

﻿using System;
using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations.Schema;
using Newtonsoft.Json.Converters;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    public partial class DexihHubUser : DexihBaseEntity
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public enum EPermission
        {
            Owner = 1,
            User = 2,
            FullReader = 3,
            PublishReader = 4,
            Suspended = 5,
            None = 6
        }

        public string UserId { get; set; }
        public long HubKey { get; set; }

        public EPermission Permission { get; set; }


        [JsonIgnore, CopyIgnore]
        public virtual DexihHub Hub { get; set; }
    }
}

﻿using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihDatalinkProfile : DexihHubNamedEntity
    {
        [Key(7)]
        [CopyParentCollectionKey]
        public long DatalinkKey { get; set; }

        [Key(8)]
        public string FunctionClassName { get; set; }

        [Key(9)]
        public string FunctionAssemblyName { get; set; }

        [Key(10)]
        public string FunctionMethodName { get; set; }

        [Key(11)]
        public bool DetailedResults { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public virtual DexihDatalink Datalink { get; set; }
        
        public override void ResetKeys()
        {
            Key = 0;
        }
    }
}

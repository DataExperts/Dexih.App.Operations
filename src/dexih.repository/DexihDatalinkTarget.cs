using System;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class DexihDatalinkTarget: DexihHubNamedEntity
    {

        [ProtoMember(1)]
        [CopyParentCollectionKey]
        public long DatalinkKey { get; set; }

        [ProtoMember(2)]
        public long? NodeDatalinkColumnKey { get; set; }

        [ProtoMember(3)]
        public DexihDatalinkColumn NodeDatalinkColumn { get; set; }

        [ProtoMember(4)]
        public int Position { get; set; }

        [ProtoMember(5)]
        public long TableKey { get; set; }
        
        [JsonIgnore, CopyIgnore]
         public DexihTable Table { get; set; }

        
        [JsonIgnore, CopyIgnore]
        public DexihDatalink Datalink { get; set; }
    }
}
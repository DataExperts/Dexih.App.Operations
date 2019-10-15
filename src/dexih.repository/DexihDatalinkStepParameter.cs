using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;

using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihDatalinkStepParameter: InputParameterBase
    {
        [Key(8)]
        [CopyParentCollectionKey]
        public long DatalinkStepKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihDatalinkStep DatalinkStep { get; set; }

    }
}
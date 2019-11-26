using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;
using MessagePack;

namespace dexih.repository
{
    /// <summary>
    /// Base class used for defining input parameters passed into datalink, view, api, jobs etc.
    /// </summary>
    [MessagePackObject]
    [Union(0, typeof(DexihApiParameter))]
    [Union(1, typeof(DexihDashboardParameter))]
    [Union(2, typeof(DexihDashboardItemParameter))]
    [Union(3, typeof(DexihDatajobParameter))]
    [Union(4, typeof(DexihDatalinkParameter))]
    [Union(5, typeof(DexihDatalinkStepParameter))]
    [Union(6, typeof(DexihViewParameter))]
    public class InputParameterBase: DexihHubNamedEntity
    {
        [Key(7)]
        public string Value { get; set; }
        
        [Key(8)]
        public long? ListOfValuesKey { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihListOfValues ListOfValues { get; set; }
        
        public override void ResetKeys()
        {
            Key = 0;
        }
    }
}
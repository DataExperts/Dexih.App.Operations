using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;


namespace dexih.repository
{
    /// <summary>
    /// Base class used for defining input parameters passed into datalink, view, api, jobs etc.
    /// </summary>
    [DataContract]
    // [Union(0, typeof(DexihApiParameter))]
    // [Union(1, typeof(DexihDashboardParameter))]
    // [Union(2, typeof(DexihDashboardItemParameter))]
    // [Union(3, typeof(DexihDatajobParameter))]
    // [Union(4, typeof(DexihDatalinkParameter))]
    // [Union(5, typeof(DexihDatalinkStepParameter))]
    // [Union(6, typeof(DexihViewParameter))]
    public class InputParameterBase: DexihHubNamedEntity
    {
        [DataMember(Order = 7)]
        public string Value { get; set; }
        
        [DataMember(Order = 8)]
        public long? ListOfValuesKey { get; set; }

        [DataMember(Order = 9)] 
        public bool AllowUserSelect { get; set; } = true;

        [DataMember(Order = 10)]
        public string ValueDesc { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihListOfValues ListOfValues { get; set; }
    }
}
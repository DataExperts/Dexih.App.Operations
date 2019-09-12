using ProtoBuf;

namespace dexih.repository
{
    /// <summary>
    /// Base class used for defining input parameters passed into datalink, view, api, jobs etc.
    /// </summary>
    [ProtoContract]
    [ProtoInclude(100, typeof(DexihApiParameter))]
    [ProtoInclude(200, typeof(DexihDashboardParameter))]
    [ProtoInclude(300, typeof(DexihDashboardItemParameter))]
    [ProtoInclude(400, typeof(DexihDatajobParameter))]
    [ProtoInclude(500, typeof(DexihDatalinkParameter))]
    [ProtoInclude(600, typeof(DexihDatalinkStepParameter))]
    [ProtoInclude(700, typeof(DexihViewParameter))]
    public class InputParameterBase: DexihHubNamedEntity
    {
        [ProtoMember(1)]
        public string Value { get; set; }
    }
}
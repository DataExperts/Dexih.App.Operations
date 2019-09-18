using dexih.functions;
using MessagePack;

namespace dexih.repository
{
    /// <summary>
    /// Base class used for defining input parameters passed into datalink, view, api, jobs etc.
    /// </summary>
    [MessagePackObject]
    [ProtoInherit(3000)]
    [MessagePack.Union(0, typeof(DexihApiParameter))]
    [MessagePack.Union(1, typeof(DexihDashboardParameter))]
    [MessagePack.Union(2, typeof(DexihDashboardItemParameter))]
    [MessagePack.Union(3, typeof(DexihDatajobParameter))]
    [MessagePack.Union(4, typeof(DexihDatalinkParameter))]
    [MessagePack.Union(5, typeof(DexihDatalinkStepParameter))]
    [MessagePack.Union(6, typeof(DexihViewParameter))]
    public class InputParameterBase: DexihHubNamedEntity
    {
        [Key(7)]
        public string Value { get; set; }
    }
}
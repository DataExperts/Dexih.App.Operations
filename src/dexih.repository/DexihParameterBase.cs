using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using ProtoBuf;
using static Dexih.Utils.DataType.DataType;

namespace dexih.repository
{
    [ProtoContract]
    [ProtoInclude(100, typeof(DexihCustomFunctionParameter))]
    [ProtoInclude(200, typeof(DexihFunctionParameterBase))]
    public class DexihParameterBase : DexihHubNamedEntity
    {


        [ProtoMember(1)]
        public int Position { get; set; } = 0;

        [ProtoMember(2)]
        public EParameterDirection Direction { get; set; }

        [ProtoMember(3)]
        public ETypeCode DataType { get; set; }

        [ProtoMember(4)]
        public bool IsGeneric { get; set; }

        [ProtoMember(5)]
        public int Rank { get; set; } = 0;

        public bool IsInput() => Direction == EParameterDirection.Input || Direction == EParameterDirection.ResultInput || Direction == EParameterDirection.Join;

        public bool IsOutput() => Direction == EParameterDirection.Output ||
                                  Direction == EParameterDirection.ResultOutput ||
                                  Direction == EParameterDirection.ReturnValue ||
                                  Direction == EParameterDirection.ResultReturnValue;

    }
}

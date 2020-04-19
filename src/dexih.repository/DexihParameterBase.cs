using System.Runtime.Serialization;
using Dexih.Utils.DataType;


namespace dexih.repository
{
    [DataContract]
    // [Union(0, typeof(DexihCustomFunctionParameter))]
    // [Union(1, typeof(DexihFunctionParameterBase))]
    public class DexihParameterBase : DexihHubNamedEntity
    {
        [DataMember(Order = 7)]
        public int Position { get; set; } = 0;

        [DataMember(Order = 8)]
        public EParameterDirection Direction { get; set; }

        [DataMember(Order = 9)]
        public ETypeCode DataType { get; set; }
        
        [DataMember(Order = 10)]
        public bool AllowNull { get; set; }

        [DataMember(Order = 11)]
        public bool IsGeneric { get; set; }

        [DataMember(Order = 12)]
        public int Rank { get; set; } = 0;

        public bool IsInput() => Direction == EParameterDirection.Input || Direction == EParameterDirection.ResultInput || Direction == EParameterDirection.Join;

        public bool IsOutput() => Direction == EParameterDirection.Output ||
                                  Direction == EParameterDirection.ResultOutput ||
                                  Direction == EParameterDirection.ReturnValue ||
                                  Direction == EParameterDirection.ResultReturnValue;
        
    }
}

using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using static Dexih.Utils.DataType.DataType;

namespace dexih.repository
{
    [Serializable]
    public class DexihParameterBase : DexihHubNamedEntity
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public enum EParameterDirection
        {
            Input, Output, ResultInput, ResultOutput, ReturnValue, ResultReturnValue, Join
        }

        public int Position { get; set; } = 0;
        
        public EParameterDirection Direction { get; set; }

        public ETypeCode DataType { get; set; }
        public bool IsGeneric { get; set; }
        
        public int Rank { get; set; } = 0;

        public bool IsInput() => Direction == EParameterDirection.Input || Direction == EParameterDirection.ResultInput || Direction == EParameterDirection.Join;

        public bool IsOutput() => Direction == EParameterDirection.Output ||
                                  Direction == EParameterDirection.ResultOutput ||
                                  Direction == EParameterDirection.ReturnValue ||
                                  Direction == EParameterDirection.ResultReturnValue;

    }
}

using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Dexih.Utils.CopyProperties;
using static Dexih.Utils.DataType.DataType;

namespace dexih.repository
{
    public class DexihParameterBase : DexihBaseEntity
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public enum EParameterDirection
        {
            Input, Output, ResultInput, ResultOutput, ReturnValue, ResultReturnValue
        }

        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }

        public string ParameterName { get; set; }

        public int Position { get; set; } = 0;
        
        public EParameterDirection Direction { get; set; }

        public ETypeCode DataType { get; set; }
        public bool IsGeneric { get; set; }
        
        public int Rank { get; set; } = 0;
    }
}

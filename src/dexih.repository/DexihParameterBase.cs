using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.ComponentModel.DataAnnotations.Schema;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using static Dexih.Utils.DataType.DataType;

namespace dexih.repository
{
    public class DexihParameterBase : DexihBaseEntity
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public enum EParameterDirection
        {
            Input, Output
        }

        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }

        public string ParameterName { get; set; }

        public int Position { get; set; } = 0;
        
        [JsonIgnore, CopyIgnore]
        public string DirectionString {
            get => Direction.ToString();
            set => Direction = (EParameterDirection)Enum.Parse(typeof(EParameterDirection), value);
        }
        [NotMapped]
        public EParameterDirection Direction { get; set; }

        [NotMapped]
        public ETypeCode DataType { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public string DataTypeString
        {
            get => DataType.ToString();
            set => DataType = (ETypeCode)Enum.Parse(typeof(ETypeCode), value);
        }
        public bool IsArray { get; set; } = false;
        public long? DatalinkColumnKey { get; set; }

    }
}

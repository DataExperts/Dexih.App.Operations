using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.ComponentModel.DataAnnotations.Schema;
using Dexih.Utils.CopyProperties;
using static Dexih.Utils.DataType.DataType;

namespace dexih.repository
{
    public partial class DexihFunctionParameter : DexihBaseEntity
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public enum EParameterDirection
        {
            Input, Output
        }

        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }

        [CopyCollectionKey((long)0, true)]
		public long FunctionParameterKey { get; set; } = 0;
		[CopyParentCollectionKey]
		public long DatalinkTransformItemKey { get; set; } = 0;
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
        public ETypeCode Datatype { get; set; }
        [JsonIgnore, CopyIgnore]
        public string DatatypeString
        {
            get => Datatype.ToString();
            set => Datatype = (ETypeCode)Enum.Parse(typeof(ETypeCode), value);
        }
        public bool IsArray { get; set; } = false;
        public long? DatalinkColumnKey { get; set; }
        public string Value { get; set; }

        [NotMapped]
        public EntityStatus EntityStatus { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihDatalinkTransformItem DtItem { get; set; }

        [CopyReference]
        public virtual DexihDatalinkColumn DatalinkColumn { get; set; }
    }
}

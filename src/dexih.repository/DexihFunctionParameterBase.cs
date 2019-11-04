using System.ComponentModel.DataAnnotations.Schema;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using MessagePack;
using Newtonsoft.Json;

namespace dexih.repository
{
    [MessagePackObject]
    [Union(0, typeof(DexihFunctionArrayParameter))]
    [Union(1, typeof(DexihFunctionParameter))]
    public class DexihFunctionParameterBase: DexihParameterBase
    {
        [Key(12)]
        public long? DatalinkColumnKey { get; set; }

        [Key(13)]
        public string Value { get; set; }

        [Key(14)]
        [NotMapped, CopyIgnore]
        public string[] ListOfValues
        {
            get
            {
                if (string.IsNullOrEmpty(ListOfValuesString))
                {
                    return null;
                }
                else
                {
                    return ListOfValuesString.Deserialize<string[]>(true);
                }
            }
            set
            {
                if (value == null)
                {
                    ListOfValuesString = null;
                }
                else
                {
                    ListOfValuesString = value.Serialize();
                }
            }
        }

        [JsonIgnore]
        public string ListOfValuesString { get; set; }

        [Key(15)]
        [NotMapped]
        public EntityStatus EntityStatus { get; set; }

        [Key(16)]
        [CopyIgnore]
        public DexihDatalinkColumn DatalinkColumn { get; set; }
    }

}
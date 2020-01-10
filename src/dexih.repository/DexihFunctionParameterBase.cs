using System.ComponentModel.DataAnnotations.Schema;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using dexih.functions;
using Dexih.Utils.CopyProperties;


namespace dexih.repository
{
    [DataContract]
    // [Union(0, typeof(DexihFunctionArrayParameter))]
    // [Union(1, typeof(DexihFunctionParameter))]
    public class DexihFunctionParameterBase: DexihParameterBase
    {
        [DataMember(Order = 13)]
        public long? DatalinkColumnKey { get; set; }

        [DataMember(Order = 14)]
        public string Value { get; set; }

        [DataMember(Order = 15)]
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

        [DataMember(Order = 16)]
        [NotMapped]
        public EntityStatus EntityStatus { get; set; }

        [DataMember(Order = 17)]
        // [CopyIgnore]
        public DexihDatalinkColumn DatalinkColumn { get; set; }
    }

}
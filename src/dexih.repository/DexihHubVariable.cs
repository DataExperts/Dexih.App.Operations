using System;
using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations.Schema;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.Crypto;

namespace dexih.repository
{
    [Serializable]
    public partial class DexihHubVariable : DexihHubNamedEntity
    {
        [NotMapped]
        public string ValueRaw { get; set; }
        public string Value { get; set; }
        public bool IsEncrypted { get; set; }
        public bool IsEnvironmentVariable { get; set; }

        public string GetValue(string key, int iterations)
        {
            string value;
            
            if (IsEncrypted)
            {
                if (String.IsNullOrEmpty(ValueRaw))
                {
                    if (String.IsNullOrEmpty(Value))
                    {
                        value = "";
                    }
                    else
                    {
                        value =  EncryptString.Decrypt(Value, key, iterations);
                    }
                }
                else
                {
                    value =  ValueRaw;
                }
            } 
            else
            {
                value =  Value;
            }
            
            if (IsEnvironmentVariable)
            {
                return Environment.GetEnvironmentVariable(value);
            }
            else
            {
                return value;
            }
        }

        public void Encrypt(string key, int iterations)
        {
            if(!String.IsNullOrEmpty(ValueRaw))
            {
                Value = EncryptString.Encrypt(ValueRaw, key, iterations);
                ValueRaw = null;
            }
        }

        [JsonIgnore, CopyIgnore]
        public virtual DexihHub Hub { get; set; }
    }
}

using System;

using System.ComponentModel.DataAnnotations.Schema;
using System.Runtime.Serialization;
using Dexih.Utils.Crypto;


namespace dexih.repository
{
    [DataContract]
    public class DexihHubVariable : DexihHubNamedEntity
    {
        [DataMember(Order = 7)]
        [NotMapped]
        public string ValueRaw { get; set; }

        [DataMember(Order = 8)]
        public string Value { get; set; }

        [DataMember(Order = 9)]
        public bool IsEncrypted { get; set; }

        [DataMember(Order = 10)]
        public bool IsEnvironmentVariable { get; set; }
        
        public string GetValue(string key, int iterations)
        {
            string value;
            
            if (IsEncrypted)
            {
                if (string.IsNullOrEmpty(ValueRaw))
                {
                    if (string.IsNullOrEmpty(Value))
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
            if(!string.IsNullOrEmpty(ValueRaw))
            {
                Value = EncryptString.Encrypt(ValueRaw, key, iterations);
                ValueRaw = null;
            }
        }

    }
}

using System;
using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations.Schema;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.Crypto;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public partial class DexihHubVariable : DexihHubNamedEntity
    {
        [ProtoMember(1)]
        [NotMapped]
        public string ValueRaw { get; set; }

        [ProtoMember(2)]
        public string Value { get; set; }

        [ProtoMember(3)]
        public bool IsEncrypted { get; set; }

        [ProtoMember(4)]
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

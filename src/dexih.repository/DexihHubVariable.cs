using System;
using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations.Schema;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.Crypto;

namespace dexih.repository
{
    public partial class DexihHubVariable : DexihBaseEntity
    {
        [CopyCollectionKey((long)0, true)]
        public long HubVariableKey { get; set; }
        public long HubKey { get; set; }

        public string Name { get; set; }

        [NotMapped]
        public string ValueRaw { get; set; }
        public string Value { get; set; }
        public bool IsEncrypted { get; set; }

        public string GetValue(string key)
        {
            if (IsEncrypted)
            {
                if (String.IsNullOrEmpty(ValueRaw))
                {
                    if (String.IsNullOrEmpty(Value))
                    {
                        return "";
                    }
                    else
                    {
                        return EncryptString.Decrypt(Value, key, 1000);
                    }
                }
                else
                {
                    return ValueRaw;
                }
            } 
            else
            {
                return Value;
            }
        }

        public void Encrypt(string key)
        {
            if(!String.IsNullOrEmpty(ValueRaw))
            {
                Value = EncryptString.Encrypt(ValueRaw, key, 1000);
                ValueRaw = null;
            }
        }

        [JsonIgnore, CopyIgnore]
        public virtual DexihHub Hub { get; set; }
    }
}

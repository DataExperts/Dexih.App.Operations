using System;

using System.ComponentModel.DataAnnotations.Schema;
using Dexih.Utils.Crypto;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihHubVariable : DexihHubNamedEntity
    {
        [Key(7)]
        [NotMapped]
        public string ValueRaw { get; set; }

        [Key(8)]
        public string Value { get; set; }

        [Key(9)]
        public bool IsEncrypted { get; set; }

        [Key(10)]
        public bool IsEnvironmentVariable { get; set; }

        public override void ResetKeys()
        {
            Key = 0;
        }
        
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

    }
}

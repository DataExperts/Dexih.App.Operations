using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using dexih.functions.Query;
using Dexih.Utils.CopyProperties;



namespace dexih.repository
{
    [DataContract]
    public class DexihApi: DexihHubNamedEntity
    {
        public DexihApi()
        {
            Parameters = new HashSet<DexihApiParameter>();
        }

        [DataMember(Order = 7)]
        public ESourceType SourceType { get; set; }

        [DataMember(Order = 8)]
        // [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceTableKey { get; set; }

        [DataMember(Order = 9)]
        // [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceDatalinkKey { get; set; }

        [DataMember(Order = 10)]
        public bool AutoStart { get; set; }

        [DataMember(Order = 11)]
        public bool CacheQueries { get; set; }

        [DataMember(Order = 12)]
        public TimeSpan? CacheResetInterval { get; set; }

        [DataMember(Order = 13)]
        public string LogDirectory { get; set; }

        [DataMember(Order = 14)]
        [CopyReference]
        public SelectQuery SelectQuery { get; set; }

        [DataMember(Order = 15)]
        public ICollection<DexihApiParameter> Parameters { get; set; }

        [DataMember(Order = 16)]
        public bool IsShared { get; set; }

        public override void ResetKeys()
        {
            Key = 0;
            
            foreach (var parameter in Parameters)
            {
                parameter.ResetKeys();
            }
        }
        
        public void UpdateParameters(InputParameters inputParameters)
        {
            if (inputParameters == null || inputParameters.Count == 0 || Parameters == null || Parameters.Count == 0)
            {
                return;
            }

            foreach (var parameter in Parameters)
            {
                var inputParameter = inputParameters.SingleOrDefault(c => c.Name == parameter.Name);
                if (inputParameter != null)
                {
                    parameter.Value = inputParameter.Value;
                }
            }
        }
    }
}
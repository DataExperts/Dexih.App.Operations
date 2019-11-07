using System;
using System.Collections.Generic;
using System.Linq;
using dexih.functions.Query;
using Dexih.Utils.CopyProperties;

using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihApi: DexihHubNamedEntity
    {
        public DexihApi()
        {
            Parameters = new HashSet<DexihApiParameter>();
        }

        [Key(7)]
        public ESourceType SourceType { get; set; }

        [Key(8)]
        // [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceTableKey { get; set; }

        [Key(9)]
        // [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceDatalinkKey { get; set; }

        [Key(10)]
        public bool AutoStart { get; set; }

        [Key(11)]
        public bool CacheQueries { get; set; }

        [Key(12)]
        public TimeSpan? CacheResetInterval { get; set; }

        [Key(13)]
        public string LogDirectory { get; set; }

        [Key(14)]
        [CopyReference]
        public SelectQuery SelectQuery { get; set; }

        [Key(15)]
        public ICollection<DexihApiParameter> Parameters { get; set; }

        [Key(16)]
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
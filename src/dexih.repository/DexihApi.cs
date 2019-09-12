using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using dexih.functions.Query;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class DexihApi: DexihHubNamedEntity
    {
        public DexihApi()
        {
            Parameters = new HashSet<DexihApiParameter>();
        }

        [ProtoMember(1)]
        public ESourceType SourceType { get; set; }

        [ProtoMember(2)]
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceTableKey { get; set; }

        [ProtoMember(3)]
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceDatalinkKey { get; set; }

        [ProtoMember(4)]
        public bool AutoStart { get; set; }

        [ProtoMember(5)]
        public bool CacheQueries { get; set; }

        [ProtoMember(6)]
        public TimeSpan? CacheResetInterval { get; set; }

        [ProtoMember(7)]
        public string LogDirectory { get; set; }

        [ProtoMember(8)]
        [CopyReference]
        public SelectQuery SelectQuery { get; set; }

        [ProtoMember(9)]
        public ICollection<DexihApiParameter> Parameters { get; set; }

        [ProtoMember(10)]
        public bool IsShared { get; set; }

        
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
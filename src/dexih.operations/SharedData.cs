using System;
using System.Collections.Generic;
using dexih.functions.Query;
using dexih.repository;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.operations
{
    public class SharedData
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public enum EObjectType
        {
            Table, Datalink, View, Dashboard, Api
        }

        public long HubKey { get; set; }
        public string HubName { get; set; }

        public EObjectType ObjectType { get; set; }
        public long ObjectKey { get; set; }
        public string Name { get; set; }
        public string LogicalName { get; set; }
        public string Description { get; set; }
        public DateTime UpdateDate { get; set; }
        
        public InputColumn[] InputColumns { get; set; }
        public IEnumerable<InputParameterBase> Parameters { get; set; }
        public SelectQuery Query { get; set; }
        public DexihColumnBase[] OutputColumns { get; set; }
    }
}
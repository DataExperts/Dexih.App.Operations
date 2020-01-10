using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using dexih.functions.Query;
using dexih.repository;




namespace dexih.operations
{

    [DataContract]
    public class SharedData
    {
        [DataMember(Order = 0)]
        public long HubKey { get; set; }

        [DataMember(Order = 1)]
        public string HubName { get; set; }

        [DataMember(Order = 2)]
        public EDataObjectType ObjectType { get; set; }

        [DataMember(Order = 3)]
        public long ObjectKey { get; set; }

        [DataMember(Order = 4)]
        public string Name { get; set; }

        [DataMember(Order = 5)]
        public string LogicalName { get; set; }

        [DataMember(Order = 6)]
        public string Description { get; set; }

        [DataMember(Order = 7)]
        public DateTime UpdateDate { get; set; }

        [DataMember(Order = 8)]
        public InputColumn[] InputColumns { get; set; }

        [DataMember(Order = 9)]
        public IEnumerable<InputParameterBase> Parameters { get; set; }

        [DataMember(Order = 10)]
        public SelectQuery Query { get; set; }

        [DataMember(Order = 11)]
        public DexihColumnBase[] OutputColumns { get; set; }
    }
}
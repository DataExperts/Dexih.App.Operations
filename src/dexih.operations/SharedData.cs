using System;
using dexih.functions.Query;
using dexih.repository;

namespace dexih.operations
{
    public class SharedData
    {
        public enum EObjectType
        {
            Table, Datalink
        }

        public long HubKey { get; set; }
        public string HubName { get; set; }

        public EObjectType ObjectType { get; set; }
        public long ObjectKey { get; set; }
        public string Name { get; set; }
        public string LogicalName { get; set; }
        public string Description { get; set; }
        public DateTime UpdateDate { get; set; }
        
        public DexihColumnBase[] InputColumns { get; set; }
        public SelectQuery Query { get; set; }
        public DexihColumnBase[] OutputColumns { get; set; }
    }
}
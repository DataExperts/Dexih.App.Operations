using Newtonsoft.Json;
using System.Collections.Generic;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    public partial class DexihStandardFunction : DexihBaseEntity
    {
        public DexihStandardFunction()
        {
            DexihDatalinkTransformItemStandardFunction = new HashSet<DexihDatalinkTransformItem>();
        }

        public long StandardFunctionKey { get; set; }
        public string Category { get; set; }
        public string Method { get; set; }
        public string ResultMethod { get; set; }
        public string Assembly { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public string CompareEnum { get; set; }
        public bool IsAggregate { get; set; }
        public bool IsCondition { get; set; }
        public bool IsRow { get; set; }
        public bool IsProfile { get; set; }
        public string InputNames { get; set; }
        public string InputTypes { get; set; }
        public string OutputNames { get; set; }
        public string OutputTypes { get; set; }
        public string ReturnType { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual ICollection<DexihDatalinkTransformItem> DexihDatalinkTransformItemStandardFunction { get; set; }

    }
}

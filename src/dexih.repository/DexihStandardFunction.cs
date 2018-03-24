//using System;
//using Newtonsoft.Json;
//using System.Collections.Generic;
//using System.ComponentModel.DataAnnotations.Schema;
//using dexih.functions.Query;
//using Dexih.Utils.CopyProperties;
//using Newtonsoft.Json.Converters;
//
//namespace dexih.repository
//{
//    public partial class DexihStandardFunction : DexihBaseEntity
//    {
//        public DexihStandardFunction()
//        {
//            DexihDatalinkTransformItemStandardFunction = new HashSet<DexihDatalinkTransformItem>();
//        }
//
//        public long StandardFunctionKey { get; set; }
//        public string Category { get; set; }
//        public string Method { get; set; }
//        public string ResultMethod { get; set; }
//        public string Assembly { get; set; }
//        public string Name { get; set; }
//        public string Description { get; set; }
//
//        [JsonIgnore, CopyIgnore]
//        public string CompareEnumString
//        {
//            get => CompareEnum == null ? null : CompareEnum.ToString();
//            set => CompareEnum = string.IsNullOrEmpty(value) ? null : (Filter.ECompare?)Enum.Parse(typeof(Filter.ECompare), value);
//        }
//
//        [NotMapped]
//        [JsonConverter(typeof(StringEnumConverter))]
//        public Filter.ECompare? CompareEnum { get; set; }
//        
//        public bool IsAggregate { get; set; }
//        public bool IsCondition { get; set; }
//        public bool IsRow { get; set; }
//        public bool IsProfile { get; set; }
//        public string InputNames { get; set; }
//        public string InputTypes { get; set; }
//        public string OutputNames { get; set; }
//        public string OutputTypes { get; set; }
//        public string ReturnType { get; set; }
//
//        [JsonIgnore, CopyIgnore]
//        public virtual ICollection<DexihDatalinkTransformItem> DexihDatalinkTransformItemStandardFunction { get; set; }
//
//    }
//}

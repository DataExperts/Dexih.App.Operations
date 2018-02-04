using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using Dexih.Utils.CopyProperties;
using dexih.transforms;
using System.Reflection;
using System.IO;

namespace dexih.repository
{
    public partial class DexihTransform : DexihBaseEntity
    {
        public DexihTransform()
        {
            DexihDatalinkTransforms = new HashSet<DexihDatalinkTransform>();
        }

        [JsonConverter(typeof(StringEnumConverter))]
        public enum ETransformType
        {
            Mapping, Filter, Sort, Group, Join, Rows, Lookup, Validation, Delta, Concatenate
        }
        public long TransformKey { get; set; }
        [NotMapped]
        public ETransformType TransformType { get; set; }
        [JsonIgnore, CopyIgnore]
        public string TransformTypeString
        {
            get { return TransformType.ToString(); }
            set { TransformType = (ETransformType)Enum.Parse(typeof(ETransformType), value); }
        }
        public string Name { get; set; }
        public string Description { get; set; }
        public string Class { get; set; }
        public string Assembly { get; set; }
        public bool RequiresTransformTable { get; set; }
        public bool AllowUserConfig { get; set; }
        public bool AllowSort { get; set; }
        public bool AllowColumnSelect { get; set; }
        public bool AllowStandardFunctions { get; set; }
        public bool AllowAggregateFunctions { get; set; }
        public bool AllowConditionFunctions { get; set; }
        public bool AllowRowFunctions { get; set; }
        public bool AllowJoin { get; set; }
        public bool AllowPassthrough { get; set; }
        public bool AllowMappingOutputs { get; set; }

        public string Icon { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkTransform> DexihDatalinkTransforms { get; set; }

        public Transform GetTransform()
        {
            Type type;
            if (Assembly == "")
            {
                type = Type.GetType(Class);
            }
            else
            {
                var assemblyName = new AssemblyName(Assembly).Name;
                var folderPath = Path.GetDirectoryName(this.GetType().GetTypeInfo().Assembly.Location);
                var assemblyPath = Path.Combine(folderPath, assemblyName + ".dll");
                if (!File.Exists(assemblyPath))
                    throw new Exception("The connection could not be started due to a missing assembly.  The assembly name is: " + Assembly + ", and class: " + Class + ", and expected in directory: " + this.GetType().GetTypeInfo().Assembly.Location + ".  Have the transforms been installed?");

                var loader = new AssemblyLoader(folderPath);
                var assembly = loader.LoadFromAssemblyName(new AssemblyName(assemblyName));

                type = assembly.GetType(Class);
            }
            var transformObject = (Transform)Activator.CreateInstance(type);
			transformObject.Name = Name;
            return transformObject;
        }
    }
}

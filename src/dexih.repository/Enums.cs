using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.repository
{
    [JsonConverter(typeof(StringEnumConverter))]
    public enum ESourceType
    {
        Datalink,
        Table,
        Rows,
        Function
    }
}
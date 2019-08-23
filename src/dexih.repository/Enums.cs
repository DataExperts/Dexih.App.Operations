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

    public enum ESharedObjectType
    {
        None,
        Connection,
        Table,
        FileFormat,
        Datalink,
        Datajob,
        DatalinkTransform,
        DatalinkTransformItem,
        RemoteAgent,
        ColumnValidation,
        TransformWriterResult,
        HubVariable,
        CustomFunction,
        DatalinkTest,
        View,
        Api,
        ApiStatus,
        Dashboard
    }
}
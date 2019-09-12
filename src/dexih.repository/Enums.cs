using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.ComponentModel;

namespace dexih.repository
{
    /// <summary>
    /// Possible source types for a datalink
    /// </summary>
    // [JsonConverter(typeof(StringEnumConverter))]
    public enum ESourceType
    {
        Datalink = 1,
        Table,
        Rows,
        Function
    }

    /// <summary>
    /// Object types which are reusable in a hub
    /// </summary>
    // [JsonConverter(typeof(StringEnumConverter))]
    public enum ESharedObjectType
    {
        Connection = 1,
        Table,
        FileFormat,
        Datalink,
        Datajob,
        RemoteAgent,
        ColumnValidation,
        HubVariable,
        CustomFunction,
        DatalinkTest,
        View,
        Api,
        Dashboard
    }

    /// <summary>
    /// Object types which can be previewed for data.
    /// </summary>
    // [JsonConverter(typeof(StringEnumConverter))]
    public enum EDataObjectType
    {
        Table = 1,
        Datalink,
        View,
        Dashboard,
        Api
    }

    // [JsonConverter(typeof(StringEnumConverter))]
    public enum EDatalinkType
    {
        [Description("Non-categorized general purpose datalink")]
        General = 1,

        [Description("Staging - loads data into a central/interim database")]
        Stage,

        [Description("Validate - performs data validation and cleaning")]
        Validate,

        [Description("Transform - reshapes, aggregates data")]
        Transform,

        [Description("Deliver - prepares data for delivering to a system/database")]
        Deliver,

        [Description("Publish - sends data to a target system/database")]
        Publish,

        [Description("Share - datalink designed to be shared with other users")]
        Share,

        [Description("Query - datalink query used for data extracts or as a source for other datalinks")]
        Query
    }

    // [JsonConverter(typeof(StringEnumConverter))]
    public enum ECleanAction
    {
        DefaultValue = 1, Truncate, Blank, Null, OriginalValue, CleanValue //action when clean is required.
    }

    // [JsonConverter(typeof(StringEnumConverter))]
    public enum EConnectionPurpose
    {
        Source = 1,
        Managed,
        Target,
        Internal
    }

    // [JsonConverter(typeof(StringEnumConverter))]
    public enum EFailAction
    {
        Continue = 1,
        ContinueNonDependent,
        Abend
    }

    public enum ETestTableAction
    {
        None = 1,
        Truncate,
        DropCreate,
        TruncateCopy,
        DropCreateCopy
    }

    // [JsonConverter(typeof(StringEnumConverter))]
    public enum ETransformItemType
    {
        BuiltInFunction = 1, CustomFunction, ColumnPair, JoinPair, Sort, Column, FilterPair, AggregatePair, Series, JoinNode, GroupNode, Node, UnGroup
    }

    /// <summary>
    /// Level of access required to view shared hub data.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum ESharedAccess
    {
        Public = 1, // shared objects can be accessed by public
        Registered, // shared objects can be accessed by regisetred users only 
        Reader // shared objects can be access only be users with PublishReader permission
    }

    // [JsonConverter(typeof(StringEnumConverter))]
    public enum EPermission
    {
        Owner = 1,
        User,
        FullReader,
        PublishReader,
        Suspended,
        None
    }

    // [JsonConverter(typeof(StringEnumConverter))]
    public enum EParameterDirection
    {
        Input = 1, Output, ResultInput, ResultOutput, ReturnValue, ResultReturnValue, Join
    }

    // [JsonConverter(typeof(StringEnumConverter))]
    public enum EViewType
    {
        Table = 1, Chart
    }

    public enum EDatabaseType
    {
        SqlServer = 1, Sqlite, Npgsql, MySql
    }

    // [JsonConverter(typeof(StringEnumConverter))]
    public enum EStatus
    {
        None = 1, Ready, Imported, Updated, Added, Error
    }

}
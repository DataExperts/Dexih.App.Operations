

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
        Dashboard,
        ListOfValues
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

    public enum ELOVObjectType
    {
        Table = 1,
        Datalink,
        View,
        Static
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
        Target
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
    public enum ESharedAccess
    {
        [Description("Shared data can be accessed by the public.")]
        Public = 1, // shared objects can be accessed by public
        
        [Description("Shared data can be accessed any registered user.")]
        Registered, // shared objects can be accessed by regisetred users only 
        
        [Description("Shared data can be accessed only by users with \"PublishReader\" permission.")]
        Reader // shared objects can be access only be users with PublishReader permission
    }

    // [JsonConverter(typeof(StringEnumConverter))]
    public enum EPermission
    {
        [Description("No access.")]
        None,

        [Description("Owner (full permissions)")]
        Owner,
        
        [Description("User (add/modify permission)")]
        User,
        
        [Description("Reader (read only access)")]
        FullReader,
        
        [Description("Publish Reader (only access shared)")]
        PublishReader,
        
        [Description("Suspended (banned from hub)")]
        Suspended,
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
    
    // [JsonConverter(typeof(StringEnumConverter))]
    public enum ELoginProvider
    {
        Dexih = 1, Google, Microsoft    
    }
    
    public enum EUserRole
    {
        Administrator = 1, Manager, User, Viewer, None
    }

    // [JsonConverter(typeof(StringEnumConverter))]
    public enum EChartType {
        BarVertical = 1,
        BarHorizontal,
        BarVertical2D,
        BarHorizontal2D,
        BarVerticalStacked,
        BarHorizontalStacked,
        BarVerticalNormalized,
        BarHorizontalNormalized,
        Pie,
        PieAdvanced,
        PieGrid,
        Line,
        Area,
        Polar,
        AreaStacked,
        AreaNormalized,
        Scatter,
        Error,
        Bubble,
        ForceDirected,
        HeatMap,
        TreeMap,
        Cards,
        Gauge,
        LinearGauge,
        Map
    }
}
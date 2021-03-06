namespace dexih.operations
{
    public enum EMessageCommand
    {
        Connect,
        Disconnect,
        MessageResponse,
        RemoteAgentUpdate,
        RemoteAgentDelete,
        RemoteAgentDeleteKey,
        HubUpdate,
        HubDelete,
        Task,
        FileDownload,
        DownloadReady,
        HubChange,
        HubError,
        DatalinkProgress,
        DatalinkStatus,
        DatajobProgress,
        DatajobStatus,
        DatalinkTestProgress,
        TableProgress,
        ApiStatus,
        ApiQuery,
        FlatFilesReady,
        RemoteMethod,
    }
    

}
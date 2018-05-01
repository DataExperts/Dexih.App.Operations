using System.Collections.Generic;
using dexih.functions;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.repository
{
    /// <summary>
    /// Class mapping of the AppSettings file used for the RemoteAgent settings.
    /// </summary>
    public class RemoteSettings
    {
        public AppSettingsSection AppSettings { get; set; } = new AppSettingsSection();
        public SystemSettingsSection SystemSettings { get; set; } = new SystemSettingsSection();
        public LoggingSection Logging { get; set; } = new LoggingSection();
    }
    
    public class AppSettingsSection
    {
        /// <summary>
        /// Indicates the remote agent is running for the first time, which will prompt user to enter settings.
        /// </summary>
        public bool FirstRun { get; set; }
        
        /// <summary>
        /// Unique ID for the remote agent.
        /// </summary>
        public string RemoteAgentId { get; set; }

        /// <summary>
        /// The user email being authenticated
        /// </summary>
        public string User { get; set; }
        
        /// <summary>
        /// The user token which authenticates the email
        /// </summary>
        public string UserToken { get; set; }
        
        /// <summary>
        /// The encryption key used for encrypting passwords, and encrypted data.
        /// </summary>
        public string EncryptionKey { get; set; }
        
        /// <summary>
        /// The Ingregation Hub Web Server: http://dexih.dataexpertsgroup.com
        /// </summary>
        public string WebServer { get; set; }
        
        /// <summary>
        /// A name to represent this remote agent.
        /// </summary>
        public string Name { get; set; }
        
        /// <summary>
        /// Auto upgrade the remote agent when a new version is available.
        /// </summary>
        public bool AutoUpgrade { get; set; }
        
        /// <summary>
        /// Allow pre-release versions to be included in the auto upgrade.
        /// </summary>
        public bool AllowPreReleases { get; set; } = false;
        
        /// <summary>
        /// Allow files and data to be downloaded through the web browser from this agent.
        /// </summary>
        public bool AllowDataDownload { get; set; }
        
        /// <summary>
        /// Allow files and data to be uploaded through the web browser from this agent.
        /// </summary>
        public bool AllowDataUpload { get; set; }

        /// <summary>
        /// Local download port to use 
        /// </summary>
        public int? DownloadPort { get; set; } = 33944; //default port

        /// <summary>
        /// Upload/download data and files directly from the remote agent.
        /// </summary>
        public bool DownloadDirectly { get; set; } = false;

        /// <summary>
        /// URL to upload/download from this agent.
        /// </summary>
        public string ExternalDownloadUrl { get; set; }
        
        /// <summary>
        /// File name of the ssl certificate
        /// </summary>
        public string PfxCertificateFilename { get; set; }
        
        /// <summary>
        /// Password for the ssl certificate
        /// </summary>
        public string PfxCertificatePassword { get; set; }
        
        /// <summary>
        /// Allow agent to read/write files to the local filesystem
        /// </summary>
        public bool AllowLocalFiles { get; set; }
        
        /// <summary>
        /// Allow agent to access files anywhere.
        /// </summary>
        public bool AllowAllPaths { get; set; }
        
        /// <summary>
        /// If AllowAllPaths = false, a list of the file paths the remote agent can access.
        /// </summary>
        public string[] AllowedPaths { get; set; }

        /// <summary>
        /// Allow agent to use any hub on the central web server.
        /// </summary>
        public bool AllowAllHubs { get; set; } = true;
        
        /// <summary>
        /// If AllowAllHubs = false, a list of the hubkeys that agent can access.
        /// </summary>
        public long[] AllowedHubs { get; set; }
                
        [JsonIgnore]
        public string Password { get; set; }
        
        [JsonIgnore]
        public string IpAddress { get; set; }

        public string GetDownloadUrl(long hubKey, string remoteAgentId, bool direct = false)
        {
            if (DownloadDirectly || direct)
            {
                if (!string.IsNullOrEmpty(ExternalDownloadUrl))
                {
                    return ExternalDownloadUrl;
                }
                else
                {
                    var url = "http://" + IpAddress + ":" + (DownloadPort??33944).ToString();
                    return url;
                }
            }
            else
            {
                var url = WebServer + $"/api/Remote/GetRemoteData/{hubKey}/{remoteAgentId}";
                return url;
            }
        }
        
        public List<FunctionReference> Functions { get; set; }
    }


    public class SystemSettingsSection
    {
        public int MaxAcknowledgeWait { get; set; } = 5000;
        public int ResponseTimeout { get; set; } = 1000_000;
        public int CancelDelay { get; set; } = 1000;
        public int EncryptionIterations { get; set; } = 1000;
        public int MaxPreviewDuration { get; set; } = 10000;
        public int MaxConcurrentTasks { get; set; } = 50;
        public long MaxUploadSize { get; set; } = 1_000_000_000;
        public TransportType SocketTransportType { get; set; } = TransportType.WebSockets;
    }

    public class LoggingSection
    {
        public bool IncludeScopes { get; set; } = false;
        public LogLevelSection LogLevel { get; set; } = new LogLevelSection();
    }

    public class LogLevelSection
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel Default { get; set; } = LogLevel.Information;
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel System { get; set; } = LogLevel.Information;
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel Microsoft { get; set; } = LogLevel.Information;
    }
    

}
using System;
using System.Collections.Generic;
using System.Linq;
using dexih.functions;
using dexih.transforms;
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
        public RuntimeSection Runtime { get; set; } = new RuntimeSection();
        public NetworkSection Network { get; set; } = new NetworkSection();
        public PrivacySection Privacy { get; set; } = new PrivacySection();
        public PermissionsSection Permissions { get; set; } = new PermissionsSection();
        public NamingStandards NamingStandards { get; set; } = new NamingStandards();

        /// <summary>
        /// Gets a list of download urls in sequence of priority
        /// </summary>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public DownloadUrl[] GetDownloadUrls(string defaultProxy)
        {
            var urls = new List<DownloadUrl>();

            if (!Privacy.AllowDataDownload && !Privacy.AllowDataUpload)
            {
                return urls.ToArray();
            }

            if (Privacy.AllowLanAccess)
            {
                if(Network.EnforceHttps && !string.IsNullOrEmpty(Network.DynamicDomain))
                {
                    urls.Add(new DownloadUrl() {
                        Url = $"https://{Runtime.LocalIpAddress.Replace('.', '-')}.{Runtime.UserHash}.{Network.DynamicDomain}:{(Network.DownloadPort ?? 33944)}",
                        DownloadUrlType = EDownloadUrlType.Direct,
                        IsEncrypted = true
                    });
                }

                if (!Network.EnforceHttps)
                {
                    urls.Add(new DownloadUrl() {
                        Url = $"http://{Runtime.LocalIpAddress}:{Network.DownloadPort ?? 33944}",
                        DownloadUrlType = EDownloadUrlType.Direct,
                        IsEncrypted = false
                    });
                }
            }

            if (Privacy.AllowExternalAccess)
            {
                if (!string.IsNullOrEmpty(Network.ExternalDownloadUrl))
                {
                    if (!Network.EnforceHttps || Network.ExternalDownloadUrl.Substring(0, 5) == "https")
                    {
                        urls.Add(new DownloadUrl() {
                            Url = Network.ExternalDownloadUrl,
                            DownloadUrlType = EDownloadUrlType.Direct,
                            IsEncrypted = true
                        });
                    }
                }
                else
                {
                    if(Network.EnforceHttps && !string.IsNullOrEmpty(Network.DynamicDomain))
                    {
                        urls.Add(new DownloadUrl() {
                            Url = $"https://{Runtime.ExternalIpAddress.Replace('.', '-')}.{Runtime.UserHash}.{Network.DynamicDomain}:{(Network.DownloadPort ?? 33944)}",
                            DownloadUrlType = EDownloadUrlType.Direct,
                            IsEncrypted = true
                        });
                    }

                    if (!Network.EnforceHttps)
                    {
                        urls.Add(new DownloadUrl() {
                            Url = $"http://{Runtime.ExternalIpAddress}:{Network.DownloadPort ?? 33944}",
                            DownloadUrlType = EDownloadUrlType.Direct,
                            IsEncrypted = false
                        });
                    }
                }
            }

            if (Privacy.AllowProxy)
            {
                var proxy = !string.IsNullOrEmpty(Network.ProxyUrl) ? Network.ProxyUrl : defaultProxy;

                if (!string.IsNullOrEmpty(proxy))
                {
                    if (Network.EnforceHttps && proxy.Substring(0, 5) == "https")
                    {
                        urls.Add(new DownloadUrl() {
                            Url = proxy,
                            DownloadUrlType = EDownloadUrlType.Proxy,
                            IsEncrypted = true
                        });
                    }

                    if (!Network.EnforceHttps)
                    {
                        urls.Add(new DownloadUrl() {
                            Url = proxy,
                            DownloadUrlType = EDownloadUrlType.Proxy,
                            IsEncrypted = false
                        });
                    }
                }
            }

            return urls.ToArray();
        }
        
        public EDataPrivacyStatus DataPrivacyStatus()
        {
            if (!Privacy.AllowDataDownload && !Privacy.AllowDataUpload)
            {
                return EDataPrivacyStatus.NotAllowed;
            }

            if (Privacy.AllowProxy)
            {
                return EDataPrivacyStatus.Proxy;
            }

            if (Privacy.AllowExternalAccess)
            {
                return EDataPrivacyStatus.Internet;
            }

            if (Privacy.AllowLanAccess)
            {
                return EDataPrivacyStatus.Lan;
            }

            return EDataPrivacyStatus.NotAllowed;
        }
    }
    
    public class AppSettingsSection
    {
        /// <summary>
        /// Indicates the remote agent is running for the first time, which will prompt user to enter settings.
        /// </summary>
        public bool FirstRun { get; set; } = true;
        
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
        public string WebServer { get; set; } = "https://dexih.dataexpertsgroup.com";
        
        /// <summary>
        /// A name to represent this remote agent.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Auto upgrade the remote agent when a new version is available.
        /// </summary>
        public bool AutoUpgrade { get; set; } = false;
        
        /// <summary>
        /// Allow pre-release versions to be included in the auto upgrade.
        /// </summary>
        public bool AllowPreReleases { get; set; } = false;

    }

    public class PermissionsSection
    {
        /// <summary>
        /// Allow agent to read/write files to the local filesystem
        /// </summary>
        public bool AllowLocalFiles { get; set; } = true;

        /// <summary>
        /// Allow agent to access files anywhere.
        /// </summary>
        public bool AllowAllPaths { get; set; } = false;
        
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

    }

    public class NetworkSection
    {
        /// <summary>
        /// URL to upload/download from this agent.
        /// </summary>
        public string ExternalDownloadUrl { get; set; }

        /// <summary>
        /// Override the default proxy server with a custom implementation.
        /// </summary>
        public string ProxyUrl { get; set; }

        /// <summary>
        /// Local download port to use 
        /// </summary>
        public int? DownloadPort { get; set; } = 33944; //default port

        /// <summary>
        /// Enforces the server to allow only https connections
        /// </summary>
        public bool EnforceHttps { get; set; } = true;

        /// <summary>
        /// Automatically generate ssl certificates
        /// </summary>
        public bool AutoGenerateCertificate { get; set; } = true;

        /// <summary>
        /// Dynamic domain used with autogenerate certificates.
        /// </summary>
        public string DynamicDomain { get; set; } = "dexih.com";
        
        /// <summary>
        /// File name of the ssl certificate
        /// </summary>
        public string CertificateFilename { get; set; }
        
        /// <summary>
        /// Password for the ssl certificate
        /// </summary>
        public string CertificatePassword { get; set; }

        /// <summary>
        /// Automatically attempts to find a UPnP device to map the port externally.
        /// </summary>
        public bool EnableUPnP { get; set; } = true;
    }

    public class PrivacySection
    {
        /// <summary>
        /// Allow files and data to be downloaded through the web browser from this agent.
        /// </summary>
        public bool AllowDataDownload { get; set; } = true;

        /// <summary>
        /// Allow files and data to be uploaded through the web browser from this agent.
        /// </summary>
        public bool AllowDataUpload { get; set; } = true;

        /// <summary>
        /// Allow files to be accessed directly through the lan.
        /// </summary>
        public bool AllowLanAccess { get; set; } = true;

        /// <summary>
        /// Allow files and data to be uploaded externally through the internet (note, ports must be mapped externally for this to work).
        /// </summary>
        public bool AllowExternalAccess { get; set; } = true;
        
        /// <summary>
        /// Allow files and data to be uploaded through a proxy.
        /// </summary>
        public bool AllowProxy { get; set; } = true;
        
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
        public string SocketTransportType { get; set; } = "WebSockets";
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

    public class RuntimeSection
    {
        public string Password { get; set; }

        public string LocalIpAddress { get; set; }

        public string ExternalIpAddress { get; set; }
        
        public string ProxyUrl { get; set; }

        public string UserHash { get; set; }
        
        public string Version { get; set; }

        public bool GenerateUserToken { get; set; } 
        
        public List<FunctionReference> Functions { get; set; }
    }


    public class NamingStandards : List<NamingStandard>
    {
        public void LoadDefault()
        {
                AddIfMissing(new NamingStandard() { Name =  "General.Table.Name", Value =  "{0}" });
                AddIfMissing(new NamingStandard() { Name =  "Stage.Table.Name", Value =  "stg{0}" });
                AddIfMissing(new NamingStandard() { Name =  "Validate.Table.Name", Value =  "val{0}" });
                AddIfMissing(new NamingStandard() { Name =  "Transform.Table.Name", Value =  "trn{0}" });
                AddIfMissing(new NamingStandard() { Name =  "Deliver.Table.Name", Value =  "{0}" });
				AddIfMissing(new NamingStandard() { Name =  "Publish.Table.Name", Value =  "{0}" });
                AddIfMissing(new NamingStandard() { Name =  "Share.Table.Name", Value =  "{0}" });
                AddIfMissing(new NamingStandard() { Name =  "General.Table.Description", Value =  "Data from the table {0}" });
				AddIfMissing(new NamingStandard() { Name =  "Stage.Table.Description", Value =  "The staging table for {0}" });
                AddIfMissing(new NamingStandard() { Name =  "Validate.Table.Description", Value =  "The validation table for {0}" });
                AddIfMissing(new NamingStandard() { Name =  "Transform.Table.Description", Value =  "The transform table for {0}" });
                AddIfMissing(new NamingStandard() { Name =  "Deliver.Table.Description", Value =  "The delivered table for {0}" });
				AddIfMissing(new NamingStandard() { Name =  "Publish.Table.Description", Value =  "The published data for {0}" });
                AddIfMissing(new NamingStandard() { Name =  "Share.Table.Description", Value =  "Data from the table {0}" });
                AddIfMissing(new NamingStandard() { Name =  "Table.RejectName", Value =  "Reject{0}" });
                AddIfMissing(new NamingStandard() { Name =  "Table.ProfileName", Value =  "Profile{0}" });
                AddIfMissing(new NamingStandard() { Name =  "General.Datalink.Name", Value =  "Data load for {0}" });
                AddIfMissing(new NamingStandard() { Name =  "Stage.Datalink.Name", Value =  "Staging load for {0}" });
                AddIfMissing(new NamingStandard() { Name =  "Validate.Datalink.Name", Value =  "Validation load for {0}" });
                AddIfMissing(new NamingStandard() { Name =  "Transform.Datalink.Name", Value =  "Transform load for {0}" });
                AddIfMissing(new NamingStandard() { Name =  "Deliver.Datalink.Name", Value =  "Deliver load for {0}" });
                AddIfMissing(new NamingStandard() { Name =  "Publish.Datalink.Name", Value =  "Publish load for {0}" });
                AddIfMissing(new NamingStandard() { Name =  "Share.Datalink.Name", Value =  "Data for {0}" });
                AddIfMissing(new NamingStandard() { Name =  "CreateDate.Column.Name", Value =  "CreateDate" });
                AddIfMissing(new NamingStandard() { Name =  "CreateDate.Column.Logical", Value =  "CreateDate" });
                AddIfMissing(new NamingStandard() { Name =  "CreateDate.Column.Description", Value =  "The date and time the record first created." });
                AddIfMissing(new NamingStandard() { Name =  "UpdateDate.Column.Name", Value =  "UpdateDate" });
                AddIfMissing(new NamingStandard() { Name =  "UpdateDate.Column.Logical", Value =  "UpdateDate" });
                AddIfMissing(new NamingStandard() { Name =  "UpdateDate.Column.Description", Value =  "The date and time the record last updated." });
                AddIfMissing(new NamingStandard() { Name =  "CreateAuditKey.Column.Name", Value =  "CreateAuditKey" });
                AddIfMissing(new NamingStandard() { Name =  "CreateAuditKey.Column.Logical", Value =  "CreateAuditKey" });
                AddIfMissing(new NamingStandard() { Name =  "CreateAuditKey.Column.Description", Value =  "Link to the audit key that created the record." });
                AddIfMissing(new NamingStandard() { Name =  "UpdateAuditKey.Column.Name", Value =  "UpdateAuditKey" });
                AddIfMissing(new NamingStandard() { Name =  "UpdateAuditKey.Column.Logical", Value =  "UpdateAuditKey" });
                AddIfMissing(new NamingStandard() { Name =  "UpdateAuditKey.Column.Description", Value =  "Link to the audit key that updated the record." });
                AddIfMissing(new NamingStandard() { Name =  "SurrogateKey.Column.Name", Value =  "{0}Sk" });
                AddIfMissing(new NamingStandard() { Name =  "SurrogateKey.Column.Logical", Value =  "{0}Sk" });
                AddIfMissing(new NamingStandard() { Name =  "SurrogateKey.Column.Description", Value =  "The surrogate key created for the table {0}." });
                AddIfMissing(new NamingStandard() { Name =  "ValidFromDate.Column.Name", Value =  "ValidFromDate" });
                AddIfMissing(new NamingStandard() { Name =  "ValidFromDate.Column.Logical", Value =  "ValidFromDate" });
                AddIfMissing(new NamingStandard() { Name =  "ValidFromDate.Column.Description", Value =  "The date and time the record becomes valid." });
                AddIfMissing(new NamingStandard() { Name =  "ValidToDate.Column.Name", Value =  "ValidToDate" });
                AddIfMissing(new NamingStandard() { Name =  "ValidToDate.Column.Logical", Value =  "ValidToDate" });
                AddIfMissing(new NamingStandard() { Name =  "ValidToDate.Column.Description", Value =  "The date and time the record becomes invalid." });
                AddIfMissing(new NamingStandard() { Name =  "IsCurrentField.Column.Name", Value =  "IsCurrent" });
                AddIfMissing(new NamingStandard() { Name =  "IsCurrentField.Column.Logical", Value =  "IsCurrent" });
				AddIfMissing(new NamingStandard() { Name =  "IsCurrentField.Column.Description", Value =  "True/False - Is the current record within the valid range?" });
                AddIfMissing(new NamingStandard() { Name =  "SourceSurrogateKey.Column.Name", Value =  "SourceSk" });
                AddIfMissing(new NamingStandard() { Name =  "SourceSurrogateKey.Column.Logical", Value =  "SourceSk" });
                AddIfMissing(new NamingStandard() { Name =  "SourceSurrogateKey.Column.Description", Value =  "The surrogate key from the source table." });
                AddIfMissing(new NamingStandard() { Name =  "ValidationStatus.Column.Name", Value =  "ValidationStatus" });
                AddIfMissing(new NamingStandard() { Name =  "ValidationStatus.Column.Logical", Value =  "ValidationStatus" });
                AddIfMissing(new NamingStandard() { Name =  "ValidationStatus.Column.Description", Value =  "Indicates if the record has passed validation tests." });
        }

        private void AddIfMissing(NamingStandard namingStandard)
        {
            if (this.All(c => c.Name != namingStandard.Name))
            {
                Add(namingStandard);
            }
        }
        
        public string ApplyNamingStandard(string name, string param1)
        {
            var namingStandard = this.SingleOrDefault(c => c.Name == name);
            if (namingStandard != null)
            {
                return namingStandard.Value.Replace("{0}", param1);
            }

            throw new Exception($"The naming standard for the name \"{name}\" with parameter \"{param1}\" could not be found.");
        }
    }

    public class NamingStandard
    {
        public string Name { get; set; }
        public string Value { get; set; }
    }
    

}
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using dexih.functions;
using Microsoft.Extensions.Logging;



using MessagePack;

namespace dexih.repository
{

    /// <summary>
    /// Class mapping of the AppSettings file used for the RemoteAgent settings.
    /// </summary>
    [MessagePackObject]
    public class RemoteSettings
    {
        [Key(0)]
        public AppSettingsSection AppSettings { get; set; } = new AppSettingsSection();

        [Key(1)]
        public SystemSettingsSection SystemSettings { get; set; } = new SystemSettingsSection();

        [Key(2)]
        public LoggingSection Logging { get; set; } = new LoggingSection();

        [Key(3)]
        public RuntimeSection Runtime { get; set; } = new RuntimeSection();

        [Key(4)]
        public NetworkSection Network { get; set; } = new NetworkSection();

        [Key(5)]
        public PrivacySection Privacy { get; set; } = new PrivacySection();

        [Key(6)]
        public PermissionsSection Permissions { get; set; } = new PermissionsSection();

        
        [Key(7)]
        public NamingStandards NamingStandards { get; set; } = new NamingStandards();

        /// <summary>
        /// Indicates if more user input is required.
        /// </summary>
        /// <returns></returns>
        public bool RequiresUserInput()
        {
            if (AppSettings.UserPrompt) return true;
            if (string.IsNullOrEmpty(AppSettings.User) || (string.IsNullOrEmpty(AppSettings.UserToken) && string.IsNullOrEmpty(Runtime.Password)))
            {
                return true;
            }
            
            return false;
        }
        
        public string CertificateFilePath()
        {
            if (string.IsNullOrEmpty(Network.CertificateFilename))
            {
                return null;
            }

            if (Path.GetFileName(Network.CertificateFilename) == Network.CertificateFilename)
            {
                return Path.Combine(Runtime.ConfigDirectory ?? "", Network.CertificateFilename);
            }

            return Network.CertificateFilename;
        }

        public string AutoStartPath()
        {
            string path;
            if (string.IsNullOrEmpty(AppSettings.AutoStartPath))
            {
                path = Path.Combine(Runtime.ConfigDirectory ?? "", "autoStart");
            }
            else
            {
                path = AppSettings.AutoStartPath;
            }

            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }

            return path;
        }

        public bool UpgradeAvailable()
        {
            if (string.IsNullOrEmpty(Runtime.LatestVersion))
            {
                return false;
            }

            if (string.CompareOrdinal( Runtime.LatestVersion, Runtime.Version) > 0)
            {
                return true;
            }

            return false;
        }
        
               /// <summary>
        /// Checks for a newer release, and downloads if there is.
        /// </summary>
        /// <returns>True is upgrade is required.</returns>
        public async Task<bool> CheckUpgrade()
        {
            var localVersion = Assembly.GetEntryAssembly().GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion;
            Runtime.Version = localVersion;
            
                string downloadUrl = null;
                string latestVersion = null;


                using (var httpClient = new HttpClient())
                {
                    httpClient.DefaultRequestHeaders.Add("User-Agent", "Dexih Remote Agent");
                    JsonElement jToken;
                    if (AppSettings.AllowPreReleases)
                    {
                        // this api gets all releases.
                        var response =
                            await httpClient.GetAsync(
                                "https://api.github.com/repos/DataExperts/Dexih.App.Remote/releases");
                        var responseText = await response.Content.ReadAsStringAsync();
                        var releases = JsonDocument.Parse(responseText);
                        // the first release will be the latest.
                        jToken = releases.RootElement[0];
                    }
                    else
                    {
                        // this api gets the latest release, excluding pre-releases.
                        var response =
                            await httpClient.GetAsync(
                                "https://api.github.com/repos/DataExperts/Dexih.App.Remote/releases/latest");
                        var responseText = await response.Content.ReadAsStringAsync();
                        jToken = JsonDocument.Parse(responseText).RootElement;
                    }

                    latestVersion = jToken.GetProperty("tag_name").GetString();

                    foreach (var asset in jToken.GetProperty("assets").EnumerateArray())
                    {
                        var name = asset.GetProperty("name").ToString().ToLower();
                        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) && name.Contains("windows"))
                        {
                            downloadUrl =  asset.GetProperty("browser_download_url").GetString();
                            break;
                        }

                        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX) && name.Contains("osx"))
                        {
                            downloadUrl = asset.GetProperty("browser_download_url").GetString();
                            break;
                        }

                        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) && name.Contains("linux"))
                        {
                            downloadUrl = asset.GetProperty("browser_download_url").GetString();
                            break;
                        }
                    }

                    Runtime.LatestVersion = latestVersion;
                    Runtime.LatestDownloadUrl = downloadUrl;

                    if (string.CompareOrdinal(latestVersion, localVersion) > 0)
                    {
                        return true;
                    }

                    return false;

                    // Download and save the update
//                        if (!string.IsNullOrEmpty(downloadUrl))
//                        {
//                            Logger.LogInformation($"Downloading latest remote agent release from {downloadUrl}.");
//                            var releaseFileName = Path.Combine(Path.GetTempPath(), "dexih.remote.latest.zip");
//
//                            if (File.Exists(releaseFileName))
//                            {
//                                File.Delete(releaseFileName);
//                            }
//
//                            using (var response = await httpClient.GetAsync(downloadUrl, HttpCompletionOption.ResponseHeadersRead))
//                            using (var streamToReadFrom = await response.Content.ReadAsStreamAsync())
//                            {
//                                using (Stream streamToWriteTo = File.Open(releaseFileName, FileMode.Create))
//                                {
//                                    await streamToReadFrom.CopyToAsync(streamToWriteTo);
//                                }
//                            }
//
//                            var extractDirectory = Path.Combine(Path.GetTempPath(), "remote.agent");
//                            if (Directory.Exists(extractDirectory))
//                            {
//                                Directory.Delete(extractDirectory, true);
//                            }
//
//                            Directory.CreateDirectory(extractDirectory);
//                            ZipFile.ExtractToDirectory(releaseFileName, extractDirectory);
//                        }
                }

        }


        /// <summary>
        /// Gets a list of download urls in sequence of priority
        /// </summary>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public DownloadUrl[] GetDownloadUrls()
        {
            var urls = new List<DownloadUrl>();

            if (!Privacy.AllowDataDownload && !Privacy.AllowDataUpload)
            {
                return urls.ToArray();
            }

            if (Privacy.AllowLanAccess)
            {
                var localIpAddress = string.IsNullOrEmpty(Network.LocalIpAddress)
                    ? Runtime.LocalIpAddress : Network.LocalIpAddress;
                var localPort = Network.LocalPort ?? Network.DownloadPort ?? 33944;

                if (Network.EnforceHttps && !string.IsNullOrEmpty(Network.DynamicDomain))
                {
                    urls.Add(new DownloadUrl()
                    {
                        Url =
                            $"https://{localIpAddress.Replace('.', '-')}.{Runtime.UserHash}.{Network.DynamicDomain}:{(localPort)}",
                        DownloadUrlType = EDownloadUrlType.Direct,
                        IsEncrypted = true
                    });
                }

                if (!Network.EnforceHttps)
                {
                    urls.Add(new DownloadUrl()
                    {
                        Url = $"http://{localIpAddress}:{localPort}",
                        DownloadUrlType = EDownloadUrlType.Direct,
                        IsEncrypted = false
                    });
                }
            }

            if (Privacy.AllowExternalAccess)
            {
                if (!string.IsNullOrEmpty(Network.ExternalDownloadUrl))
                {
                    bool encrypted = Network.ExternalDownloadUrl.StartsWith("https:://");
                    if (!Network.EnforceHttps || encrypted)
                    {
                        urls.Add(new DownloadUrl() {
                            Url = Network.ExternalDownloadUrl,
                            DownloadUrlType = EDownloadUrlType.Direct,
                            IsEncrypted = encrypted
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
                var proxy = !string.IsNullOrEmpty(Network.ProxyUrl) ? Network.ProxyUrl : Runtime.DefaultProxyUrl;

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
    
    [MessagePackObject]
    public class AppSettingsSection
    {
        /// <summary>
        /// Indicates the remote agent is running for the first time, which will prompt user to enter settings.
        /// </summary>
        [Key(0)]
        public bool UserPrompt { get; set; } = true;

        /// <summary>
        /// Unique ID for the remote agent.
        /// </summary>
        [Key(1)]
        public string RemoteAgentId { get; set; }

        /// <summary>
        /// The user email being authenticated
        /// </summary>
        [Key(2)]
        public string User { get; set; }

        /// <summary>
        /// The user token which authenticates the email
        /// </summary>
        [Key(3)]
        public string UserToken { get; set; }

        /// <summary>
        /// The encryption key used for encrypting passwords, and encrypted data.
        /// </summary>
        [Key(4)]
        public string EncryptionKey { get; set; }

        /// <summary>
        /// The Ingregation Hub Web Server: http://dexih.com
        /// </summary>
        [Key(5)]
        public string WebServer { get; set; } = "https://dexih.com";

        /// <summary>
        /// A name to represent this remote agent.
        /// </summary>
        [Key(6)]
        public string Name { get; set; }

        /// <summary>
        /// Auto upgrade the remote agent when a new version is available.
        /// </summary>
        [Key(7)]
        public bool AutoUpgrade { get; set; } = false;

        /// <summary>
        /// Allow pre-release versions to be included in the auto upgrade.
        /// </summary>
        [Key(8)]
        public bool AllowPreReleases { get; set; } = false;

        [Key(9)]
        public string AutoStartPath { get; set; }

    }

    [MessagePackObject]
    public class PermissionsSection
    {
        /// <summary>
        /// Allow agent to read/write files to the local filesystem
        /// </summary>
        [Key(0)]
        public bool AllowLocalFiles { get; set; } = true;

        /// <summary>
        /// Allow agent to access files anywhere.
        /// </summary>
        [Key(1)]
        public bool AllowAllPaths { get; set; } = false;

        /// <summary>
        /// If AllowAllPaths = false, a list of the file paths the remote agent can access.
        /// </summary>
        [Key(2)]
        public string[] AllowedPaths { get; set; } = null;

        /// <summary>
        /// Allow agent to use any hub on the central web server.
        /// </summary>
        [Key(3)]
        public bool AllowAllHubs { get; set; } = true;

        /// <summary>
        /// If AllowAllHubs = false, a list of the hubkeys that agent can access.
        /// </summary>
        [Key(4)]
        public long[] AllowedHubs { get; set; } = null;

        public FilePermissions GetFilePermissions()
        {
            return new FilePermissions()
            {
                AllowedPaths = AllowedPaths,
                AllowAllPaths = AllowAllPaths,
                AllowLocalFiles = AllowLocalFiles
            };
        }
    }

    [MessagePackObject]
    public class NetworkSection
    {
        /// <summary>
        /// URL to upload/download from this agent.
        /// </summary>
        [Key(0)]
        public string ExternalDownloadUrl { get; set; }

        /// <summary>
        /// Local IP to upload/download from this agent
        /// </summary>
        [Key(1)]
        public string LocalIpAddress { get; set; }

        /// <summary>
        /// Local port to upload/download  
        /// </summary>
        [Key(2)]
        public int? LocalPort { get; set; }

        /// <summary>
        /// Override the default proxy server with a custom implementation.
        /// </summary>
        [Key(3)]
        public string ProxyUrl { get; set; }

        /// <summary>
        /// Download port to use 
        /// </summary>
        [Key(4)]
        public int? DownloadPort { get; set; } = 33944; //default port

        /// <summary>
        /// Enforces the server to allow only https connections
        /// </summary>
        [Key(5)]
        public bool EnforceHttps { get; set; } = true;

        /// <summary>
        /// Automatically generate ssl certificates
        /// </summary>
        [Key(6)]
        public bool AutoGenerateCertificate { get; set; } = true;

        /// <summary>
        /// Dynamic domain used with autogenerate certificates.
        /// </summary>
        [Key(7)]
        public string DynamicDomain { get; set; } = "dexih.com";

        /// <summary>
        /// File name of the ssl certificate
        /// </summary>
        [Key(8)]
        public string CertificateFilename { get; set; }


        /// <summary>
        /// Password for the ssl certificate
        /// </summary>
        [Key(9)]
        public string CertificatePassword { get; set; }

        /// <summary>
        /// Automatically attempts to find a UPnP device to map the port externally.
        /// </summary>
        [Key(10)]
        public bool EnableUPnP { get; set; } = true;
    }

    [MessagePackObject]
    public class PrivacySection
    {
        /// <summary>
        /// Allow files and data to be downloaded through the web browser from this agent.
        /// </summary>
        [Key(0)]
        public bool AllowDataDownload { get; set; } = true;

        /// <summary>
        /// Allow files and data to be uploaded through the web browser from this agent.
        /// </summary>
        [Key(1)]
        public bool AllowDataUpload { get; set; } = true;

        /// <summary>
        /// Allow files to be accessed directly through the lan.
        /// </summary>
        [Key(2)]
        public bool AllowLanAccess { get; set; } = true;

        /// <summary>
        /// Allow files and data to be uploaded externally through the internet (note, ports must be mapped externally for this to work).
        /// </summary>
        [Key(3)]
        public bool AllowExternalAccess { get; set; } = true;

        /// <summary>
        /// Allow files and data to be uploaded through a proxy.
        /// </summary>
        [Key(4)]
        public bool AllowProxy { get; set; } = true;
        
    }

    [MessagePackObject]
    public class SystemSettingsSection
    {
        [Key(0)]
        public int MaxAcknowledgeWait { get; set; } = 5000;

        [Key(1)]
        public int ResponseTimeout { get; set; } = 1000_000;

        [Key(2)]
        public int CancelDelay { get; set; } = 1000;

        [Key(3)]
        public int EncryptionIterations { get; set; } = 1000;

        [Key(4)]
        public int MaxPreviewDuration { get; set; } = 10000;

        [Key(5)]
        public int MaxConcurrentTasks { get; set; } = 50;

        [Key(6)]
        public long MaxUploadSize { get; set; } = 1_000_000_000;

        [Key(7)]
        public string SocketTransportType { get; set; } = "WebSockets";
    }

    [MessagePackObject]
    public class LoggingSection
    {
        [Key(0)]
        public bool IncludeScopes { get; set; } = false;

        [Key(1)]
        public LogLevelSection LogLevel { get; set; } = new LogLevelSection();

        /// <summary>
        /// File name to create a log file
        /// </summary>
        [Key(2)]
        public string LogFilePath { get; set; }
    }

    [MessagePackObject]
    public class LogLevelSection
    {
        [Key(0)]
        // [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel Default { get; set; } = LogLevel.Information;

        // [JsonConverter(typeof(StringEnumConverter))]
        [Key(1)]
        public LogLevel System { get; set; } = LogLevel.Information;

        // [JsonConverter(typeof(StringEnumConverter))]
        [Key(2)]
        public LogLevel Microsoft { get; set; } = LogLevel.Information;

    }

    [MessagePackObject]
    public class RuntimeSection
    {
        [Key(0)]
        public string ConfigDirectory { get; set; }

        [Key(1)]
        public string AppSettingsPath { get; set; }

        [Key(2)]
        public string Password { get; set; }

        [Key(3)]
        public string LocalIpAddress { get; set; }
        //        public string LocalPort { get; set; }

        [Key(4)]
        public string ExternalIpAddress { get; set; }

        [Key(5)]
        public string DefaultProxyUrl { get; set; }

        [Key(6)]
        public long RemoteAgentKey { get; set; }
        
        [Key(7)]
        public string UserHash { get; set; }

        [Key(8)]
        public string Version { get; set; }

        [Key(9)]
        public string LatestVersion { get; set; }

        [Key(10)]
        public string LatestDownloadUrl { get; set; }

        [Key(11)]
        public bool GenerateUserToken { get; set; }

        [Key(12)]
        public bool SaveSettings { get; set; }

        [Key(13)]
        public bool DoUpgrade { get; set; } = false;

//        public List<FunctionReference> Functions { get; set; }
    }


    [MessagePackObject]
    public class NamingStandards : Dictionary<string, string>
    {
        public void LoadDefault()
        {
                AddIfMissing("General.Table.Name",  "{0}" );
                AddIfMissing("Stage.Table.Name",  "stg{0}" );
                AddIfMissing("Validate.Table.Name",  "val{0}" );
                AddIfMissing("Transform.Table.Name",  "trn{0}" );
                AddIfMissing("Deliver.Table.Name",  "{0}" );
				AddIfMissing("Publish.Table.Name",  "{0}" );
                AddIfMissing("Share.Table.Name",  "{0}" );
                AddIfMissing("General.Table.Description",  "Data from the table {0}" );
				AddIfMissing("Stage.Table.Description",  "The staging table for {0}" );
                AddIfMissing("Validate.Table.Description",  "The validation table for {0}" );
                AddIfMissing("Transform.Table.Description",  "The transform table for {0}" );
                AddIfMissing("Deliver.Table.Description",  "The delivered table for {0}" );
				AddIfMissing("Publish.Table.Description",  "The published data for {0}" );
                AddIfMissing("Share.Table.Description",  "Data from the table {0}" );
                AddIfMissing("Table.RejectName",  "Reject{0}" );
                AddIfMissing("Table.ProfileName",  "Profile{0}" );
                AddIfMissing("General.Datalink.Name",  "Data load for {0}" );
                AddIfMissing("Stage.Datalink.Name",  "Staging load for {0}" );
                AddIfMissing("Validate.Datalink.Name",  "Validation load for {0}" );
                AddIfMissing("Transform.Datalink.Name",  "Transform load for {0}" );
                AddIfMissing("Deliver.Datalink.Name",  "Deliver load for {0}" );
                AddIfMissing("Publish.Datalink.Name",  "Publish load for {0}" );
                AddIfMissing("Share.Datalink.Name",  "Data for {0}" );
                AddIfMissing("CreateDate.Column.Name",  "CreateDate" );
                AddIfMissing("CreateDate.Column.Logical",  "CreateDate" );
                AddIfMissing("CreateDate.Column.Description",  "The date and time the record first created." );
                AddIfMissing("UpdateDate.Column.Name",  "UpdateDate" );
                AddIfMissing("UpdateDate.Column.Logical",  "UpdateDate" );
                AddIfMissing("UpdateDate.Column.Description",  "The date and time the record last updated." );
                AddIfMissing("CreateAuditKey.Column.Name",  "CreateAuditKey" );
                AddIfMissing("CreateAuditKey.Column.Logical",  "CreateAuditKey" );
                AddIfMissing("CreateAuditKey.Column.Description",  "Link to the audit key that created the record." );
                AddIfMissing("UpdateAuditKey.Column.Name",  "UpdateAuditKey" );
                AddIfMissing("UpdateAuditKey.Column.Logical",  "UpdateAuditKey" );
                AddIfMissing("UpdateAuditKey.Column.Description",  "Link to the audit key that updated the record." );
                AddIfMissing("AutoIncrement.Column.Name",  "{0}Sk" );
                AddIfMissing("AutoIncrement.Column.Logical",  "{0}Sk" );
                AddIfMissing("AutoIncrement.Column.Description",  "The surrogate key created for the table {0}." );
                AddIfMissing("ValidFromDate.Column.Name",  "ValidFromDate" );
                AddIfMissing("ValidFromDate.Column.Logical",  "ValidFromDate" );
                AddIfMissing("ValidFromDate.Column.Description",  "The date and time the record becomes valid." );
                AddIfMissing("ValidToDate.Column.Name",  "ValidToDate" );
                AddIfMissing("ValidToDate.Column.Logical",  "ValidToDate" );
                AddIfMissing("ValidToDate.Column.Description",  "The date and time the record becomes invalid." );
                AddIfMissing("IsCurrentField.Column.Name",  "IsCurrent" );
                AddIfMissing("IsCurrentField.Column.Logical",  "IsCurrent" );
				AddIfMissing("IsCurrentField.Column.Description",  "True/False - Is the current record within the valid range?" );
                AddIfMissing("Version.Column.Name",  "Version" );
                AddIfMissing("Version.Column.Logical",  "Version" );
                AddIfMissing("Version.Column.Description",  "Version number of preserved records." );
                AddIfMissing("SourceSurrogateKey.Column.Name",  "SourceSk" );
                AddIfMissing("SourceSurrogateKey.Column.Logical",  "SourceSk" );
                AddIfMissing("SourceSurrogateKey.Column.Description",  "The surrogate key from the source table." );
                AddIfMissing("ValidationStatus.Column.Name",  "ValidationStatus" );
                AddIfMissing("ValidationStatus.Column.Logical",  "ValidationStatus" );
                AddIfMissing("ValidationStatus.Column.Description",  "Indicates if the record has passed validation tests." );
        }

        private void AddIfMissing(string name, string value)
        {
            if (!ContainsKey(name))
            {
                Add(name, value);
            }
        }

        private bool _defaultLoaded;
        
        public string ApplyNamingStandard(string name, string param1)
        {
            if (!_defaultLoaded)
            {
                LoadDefault();
                _defaultLoaded = true;
            }
            
            var namingStandard = this[name];
            if (namingStandard != null)
            {
                return namingStandard.Replace("{0}", param1);
            }

            throw new Exception($"The naming standard for the name \"{name}\" with parameter \"{param1}\" could not be found.");
        }
    }


    

}
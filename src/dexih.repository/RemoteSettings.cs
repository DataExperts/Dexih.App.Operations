using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Runtime.Serialization;
using System.Text.Json;
using System.Threading.Tasks;
using dexih.functions;
using Microsoft.Extensions.Logging;





namespace dexih.repository
{

    /// <summary>
    /// Class mapping of the AppSettings file used for the RemoteAgent settings.
    /// </summary>
    [DataContract]
    public class RemoteSettings
    {
        [DataMember(Order = 0)] public AppSettingsSection AppSettings { get; set; } = new AppSettingsSection();

        [DataMember(Order = 1)] public SystemSettingsSection SystemSettings { get; set; } = new SystemSettingsSection();

        [DataMember(Order = 2)] public LoggingSection Logging { get; set; } = new LoggingSection();

        [DataMember(Order = 3)] public RuntimeSection Runtime { get; set; } = new RuntimeSection();

        [DataMember(Order = 4)] public NetworkSection Network { get; set; } = new NetworkSection();

        [DataMember(Order = 5)] public PrivacySection Privacy { get; set; } = new PrivacySection();

        [DataMember(Order = 6)] public PermissionsSection Permissions { get; set; } = new PermissionsSection();


        [DataMember(Order = 7)] public NamingStandards NamingStandards { get; set; } = new NamingStandards();

        [DataMember(Order = 8)] public PluginsSection Plugins { get; set; } = new PluginsSection();
        
        /// <summary>
        /// Indicates if more user input is required.
        /// </summary>
        /// <returns></returns>
        public bool RequiresUserInput()
        {
            if (AppSettings.UserPrompt) return true;
            if (string.IsNullOrEmpty(AppSettings.User) ||
                (string.IsNullOrEmpty(AppSettings.UserToken) && string.IsNullOrEmpty(Runtime.Password)))
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

            if (string.CompareOrdinal(Runtime.LatestVersion, Runtime.Version) > 0)
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
                        downloadUrl = asset.GetProperty("browser_download_url").GetString();
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

                if (string.CompareOrdinal(latestVersion, Runtime.Version) > 0)
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
                    ? Runtime.LocalIpAddress
                    : Network.LocalIpAddress;
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
                        urls.Add(new DownloadUrl()
                        {
                            Url = Network.ExternalDownloadUrl,
                            DownloadUrlType = EDownloadUrlType.Direct,
                            IsEncrypted = encrypted
                        });
                    }
                }
                else
                {
                    if (Network.EnforceHttps && !string.IsNullOrEmpty(Network.DynamicDomain))
                    {
                        urls.Add(new DownloadUrl()
                        {
                            Url =
                                $"https://{Runtime.ExternalIpAddress.Replace('.', '-')}.{Runtime.UserHash}.{Network.DynamicDomain}:{(Network.DownloadPort ?? 33944)}",
                            DownloadUrlType = EDownloadUrlType.Direct,
                            IsEncrypted = true
                        });
                    }

                    if (!Network.EnforceHttps)
                    {
                        urls.Add(new DownloadUrl()
                        {
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
                        urls.Add(new DownloadUrl()
                        {
                            Url = proxy,
                            DownloadUrlType = EDownloadUrlType.Proxy,
                            IsEncrypted = true
                        });
                    }

                    if (!Network.EnforceHttps)
                    {
                        urls.Add(new DownloadUrl()
                        {
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

        public async Task GetPlugins(ILogger logger = null)
        {
            using (var httpClient = new HttpClient())
            {
                logger.LogInformation("Checking for missing plugins...");
                
                httpClient.DefaultRequestHeaders.Add("User-Agent", "Dexih Remote Agent");
                JsonElement jToken;
                if (AppSettings.AllowPreReleases)
                {
                    // this api gets all releases.
                    var response =
                        await httpClient.GetAsync(
                            "https://api.github.com/repos/DataExperts/dexih.transforms/releases");
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
                            "https://api.github.com/repos/DataExperts/dexih.transforms/releases/latest");
                    var responseText = await response.Content.ReadAsStringAsync();
                    jToken = JsonDocument.Parse(responseText).RootElement;
                }

                var latestVersion = jToken.GetProperty("tag_name").GetString();

                var pluginDirectory = Path.Combine(Directory.GetCurrentDirectory(), "plugins");
                if (Directory.Exists(pluginDirectory))
                {
                    Directory.CreateDirectory(pluginDirectory);
                }

                List<string> installed = new List<string>();

                var installedUpdated = false;
                var installedFile = Path.Combine(pluginDirectory, "installed.txt");

                if (File.Exists(installedFile))
                {
                    installed.AddRange(await File.ReadAllLinesAsync(installedFile));
                }

                foreach (var asset in jToken.GetProperty("assets").EnumerateArray())
                {
                    var download = false;
                    var name = asset.GetProperty("name").ToString().ToLower();
                    
                    var downloadPath = Path.Combine(pluginDirectory, name);

                    if (File.Exists(downloadPath))
                    {
                        continue;
                    }

                    if (Plugins.MLNet && name.StartsWith("dexih.functions.ml"))
                    {
                        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) && name.Contains("windows"))
                        {
                            download = true;
                        }

                        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX) && name.Contains("osx"))
                        {
                            download = true;
                        }

                        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) && name.Contains("linux"))
                        {
                            download = true;
                        }
                    }

                    if (Plugins.DB2 && name.StartsWith("dexih.connections.db2"))
                    {
                        download = true;
                    }

                    if (Plugins.Excel && name.StartsWith("dexih.connections.excel"))
                    {
                        download = true;
                    }

                    if (Plugins.Mongo && name.StartsWith("dexih.connections.mongo"))
                    {
                        download = true;
                    }

                    if (Plugins.Oracle && name.StartsWith("dexih.connections.oracle"))
                    {
                        download = true;
                    }
                    
                    if (download)
                    {
                        if (installed.Contains(name + "/" + latestVersion))
                        {
                            continue;
                        }

                        logger.LogInformation($"Downloading plugin {name}");
                        var downloadUrl = asset.GetProperty("browser_download_url").GetString();
                        // Download and save the update
                        if (!string.IsNullOrEmpty(downloadUrl))
                        {
                            using (var response = await httpClient.GetAsync(downloadUrl,
                                HttpCompletionOption.ResponseHeadersRead))
                            using (var streamToReadFrom = await response.Content.ReadAsStreamAsync())
                            {
                                using (Stream streamToWriteTo = File.Open(downloadPath, FileMode.Create))
                                {
                                    await streamToReadFrom.CopyToAsync(streamToWriteTo);
                                }
                            }

                            logger.LogInformation($"Extracting plugin {name}");

                            var extractDirectory = Path.Combine(Directory.GetCurrentDirectory(), "plugins", "standard");
                            if (!Directory.Exists(extractDirectory))
                            {
                                Directory.CreateDirectory(extractDirectory);
                            }

                            ZipFile.ExtractToDirectory(downloadPath, extractDirectory, true);

                            installedUpdated = true;
                            installed.Add(name + "/" + latestVersion);
                            
                            File.Delete(downloadPath);
                        }
                    }
                }

                if (installedUpdated)
                {
                    File.WriteAllLinesAsync(installedFile, installed);
                }

                logger.LogInformation($"Download plugins complete.");
            }
        }
    }

    [DataContract]
    public class AppSettingsSection
    {
        /// <summary>
        /// Indicates the remote agent is running for the first time, which will prompt user to enter settings.
        /// </summary>
        [DataMember(Order = 0)]
        public bool UserPrompt { get; set; } = true;

        /// <summary>
        /// Unique ID for the remote agent.
        /// </summary>
        [DataMember(Order = 1)]
        public string RemoteAgentId { get; set; }

        /// <summary>
        /// The user email being authenticated
        /// </summary>
        [DataMember(Order = 2)]
        public string User { get; set; }

        /// <summary>
        /// The user token which authenticates the email
        /// </summary>
        [DataMember(Order = 3)]
        public string UserToken { get; set; }

        /// <summary>
        /// The encryption key used for encrypting passwords, and encrypted data.
        /// </summary>
        [DataMember(Order = 4)]
        public string EncryptionKey { get; set; }

        /// <summary>
        /// The Ingregation Hub Web Server: http://dexih.com
        /// </summary>
        [DataMember(Order = 5)]
        public string WebServer { get; set; } = "https://dexih.com";

        /// <summary>
        /// A name to represent this remote agent.
        /// </summary>
        [DataMember(Order = 6)]
        public string Name { get; set; }

        /// <summary>
        /// Auto upgrade the remote agent when a new version is available.
        /// </summary>
        [DataMember(Order = 7)]
        public bool AutoUpgrade { get; set; } = false;

        /// <summary>
        /// Allow pre-release versions to be included in the auto upgrade.
        /// </summary>
        [DataMember(Order = 8)]
        public bool AllowPreReleases { get; set; } = false;

        [DataMember(Order = 9)]
        public string AutoStartPath { get; set; }
        
    }

    [DataContract]
    public class PluginsSection
    {
        [DataMember(Order = 0)]
        public bool MLNet { get; set; } = false;

        [DataMember(Order = 1)]
        public bool Excel { get; set; } = false;

        [DataMember(Order = 2)]
        public bool Oracle { get; set; } = false;

        [DataMember(Order = 3)]
        public bool DB2 { get; set; } = false;

        [DataMember(Order = 4)]
        public bool Mongo { get; set; } = false;

    }

    [DataContract]
    public class PermissionsSection
    {
        /// <summary>
        /// Allow agent to read/write files to the local filesystem
        /// </summary>
        [DataMember(Order = 0)]
        public bool AllowLocalFiles { get; set; } = true;

        /// <summary>
        /// Allow agent to access files anywhere.
        /// </summary>
        [DataMember(Order = 1)]
        public bool AllowAllPaths { get; set; } = false;

        /// <summary>
        /// If AllowAllPaths = false, a list of the file paths the remote agent can access.
        /// </summary>
        [DataMember(Order = 2)]
        public string[] AllowedPaths { get; set; } = null;

        /// <summary>
        /// Allow agent to use any hub on the central web server.
        /// </summary>
        [DataMember(Order = 3)]
        public bool AllowAllHubs { get; set; } = true;

        /// <summary>
        /// If AllowAllHubs = false, a list of the hubkeys that agent can access.
        /// </summary>
        [DataMember(Order = 4)]
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

    [DataContract]
    public class NetworkSection
    {
        /// <summary>
        /// URL to upload/download from this agent.
        /// </summary>
        [DataMember(Order = 0)]
        public string ExternalDownloadUrl { get; set; }

        /// <summary>
        /// Local IP to upload/download from this agent
        /// </summary>
        [DataMember(Order = 1)]
        public string LocalIpAddress { get; set; }

        /// <summary>
        /// Local port to upload/download  
        /// </summary>
        [DataMember(Order = 2)]
        public int? LocalPort { get; set; }

        /// <summary>
        /// Override the default proxy server with a custom implementation.
        /// </summary>
        [DataMember(Order = 3)]
        public string ProxyUrl { get; set; }

        /// <summary>
        /// Download port to use 
        /// </summary>
        [DataMember(Order = 4)]
        public int? DownloadPort { get; set; } = 33944; //default port

        /// <summary>
        /// Enforces the server to allow only https connections
        /// </summary>
        [DataMember(Order = 5)]
        public bool EnforceHttps { get; set; } = true;

        /// <summary>
        /// Automatically generate ssl certificates
        /// </summary>
        [DataMember(Order = 6)]
        public bool AutoGenerateCertificate { get; set; } = true;

        /// <summary>
        /// Dynamic domain used with autogenerate certificates.
        /// </summary>
        [DataMember(Order = 7)]
        public string DynamicDomain { get; set; } = "dexih.com";

        /// <summary>
        /// File name of the ssl certificate
        /// </summary>
        [DataMember(Order = 8)]
        public string CertificateFilename { get; set; }


        /// <summary>
        /// Password for the ssl certificate
        /// </summary>
        [DataMember(Order = 9)]
        public string CertificatePassword { get; set; }

        /// <summary>
        /// Automatically attempts to find a UPnP device to map the port externally.
        /// </summary>
        [DataMember(Order = 10)]
        public bool EnableUPnP { get; set; } = true;
    }

    [DataContract]
    public class PrivacySection
    {
        /// <summary>
        /// Allow files and data to be downloaded through the web browser from this agent.
        /// </summary>
        [DataMember(Order = 0)]
        public bool AllowDataDownload { get; set; } = true;

        /// <summary>
        /// Allow files and data to be uploaded through the web browser from this agent.
        /// </summary>
        [DataMember(Order = 1)]
        public bool AllowDataUpload { get; set; } = true;

        /// <summary>
        /// Allow files to be accessed directly through the lan.
        /// </summary>
        [DataMember(Order = 2)]
        public bool AllowLanAccess { get; set; } = true;

        /// <summary>
        /// Allow files and data to be uploaded externally through the internet (note, ports must be mapped externally for this to work).
        /// </summary>
        [DataMember(Order = 3)]
        public bool AllowExternalAccess { get; set; } = true;

        /// <summary>
        /// Allow files and data to be uploaded through a proxy.
        /// </summary>
        [DataMember(Order = 4)]
        public bool AllowProxy { get; set; } = true;
        
    }

    [DataContract]
    public class SystemSettingsSection
    {
        [DataMember(Order = 0)]
        public int MaxAcknowledgeWait { get; set; } = 5000;

        [DataMember(Order = 1)]
        public int ResponseTimeout { get; set; } = 1000_000;

        [DataMember(Order = 2)]
        public int CancelDelay { get; set; } = 1000;

        [DataMember(Order = 3)]
        public int EncryptionIterations { get; set; } = 1000;

        [DataMember(Order = 4)]
        public int MaxPreviewDuration { get; set; } = 10000;

        [DataMember(Order = 5)]
        public int MaxConcurrentTasks { get; set; } = 50;

        [DataMember(Order = 6)]
        public long MaxUploadSize { get; set; } = 1_000_000_000;

        [DataMember(Order = 7)]
        public string SocketTransportType { get; set; } = "WebSockets";
    }

    [DataContract]
    public class LoggingSection
    {
        [DataMember(Order = 0)]
        public bool IncludeScopes { get; set; } = false;

        [DataMember(Order = 1)]
        public LogLevelSection LogLevel { get; set; } = new LogLevelSection();

        /// <summary>
        /// File name to create a log file
        /// </summary>
        [DataMember(Order = 2)]
        public string LogFilePath { get; set; }
    }

    [DataContract]
    public class LogLevelSection
    {
        [DataMember(Order = 0)]
        // [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel Default { get; set; } = LogLevel.Information;

        // [JsonConverter(typeof(StringEnumConverter))]
        [DataMember(Order = 1)]
        public LogLevel System { get; set; } = LogLevel.Information;

        // [JsonConverter(typeof(StringEnumConverter))]
        [DataMember(Order = 2)]
        public LogLevel Microsoft { get; set; } = LogLevel.Information;

    }

    [DataContract]
    public class RuntimeSection
    {
        [DataMember(Order = 0)]
        public string ConfigDirectory { get; set; }

        [DataMember(Order = 1)]
        public string AppSettingsPath { get; set; }

        [DataMember(Order = 2)]
        public string Password { get; set; }

        [DataMember(Order = 3)]
        public string LocalIpAddress { get; set; }
        //        public string LocalPort { get; set; }

        [DataMember(Order = 4)]
        public string ExternalIpAddress { get; set; }

        [DataMember(Order = 5)]
        public string DefaultProxyUrl { get; set; }

        [DataMember(Order = 6)]
        public long RemoteAgentKey { get; set; }
        
        [DataMember(Order = 7)]
        public string UserHash { get; set; }

        [DataMember(Order = 8)]
        public string Version { get; set; }

        [DataMember(Order = 9)]
        public string LatestVersion { get; set; }

        [DataMember(Order = 10)]
        public string LatestDownloadUrl { get; set; }

        [DataMember(Order = 11)]
        public bool GenerateUserToken { get; set; }

        [DataMember(Order = 12)]
        public bool SaveSettings { get; set; }

        [DataMember(Order = 13)]
        public bool DoUpgrade { get; set; } = false;

//        public List<FunctionReference> Functions { get; set; }
    }


   


    

}
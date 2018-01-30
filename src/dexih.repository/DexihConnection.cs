using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.ComponentModel.DataAnnotations.Schema;
using Dexih.Utils.Crypto;
using Dexih.Utils.CopyProperties;
using dexih.transforms;
using System.Reflection;
using System.IO;
using System.Linq;

namespace dexih.repository
{
    public partial class DexihConnection : DexihBaseEntity
    {
        #region Enums
        [JsonConverter(typeof(StringEnumConverter))]
        public enum EConnectionPurpose
        {
            Source = 0,
            Managed = 1,
            Target = 2,
            Internal = 3
        }
         
        #endregion

        public DexihConnection()
        {
            DexihTables = new HashSet<DexihTable>();
        }

        [CopyCollectionKey((long)0, true)]
        public long ConnectionKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }
        public long DatabaseTypeKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public string PurposeString 
        {
            get => Purpose.ToString();
            private set => Purpose = (EConnectionPurpose)Enum.Parse(typeof(EConnectionPurpose), value);
        }
        
        [NotMapped]
        public EConnectionPurpose Purpose { get; set; }

        public string Name { get; set; }
        public string Server { get; set; }
        public bool UseWindowsAuth { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public bool UsePasswordVariable { get; set; }
        public string DefaultDatabase { get; set; }
        public string Filename { get; set; }
        public bool UseConnectionString { get; set; }
        public string ConnectionString { get; set; }
        public bool UseConnectionStringVariable { get; set; }
        public bool IsInternal { get; set; }
        public bool EmbedTableKey { get; set; }
 
        //these store the raw (unencrypted values) and are not saved to the database.
        [NotMapped]
        public string PasswordRaw { get; set; }
        [NotMapped]
        public string ConnectionStringRaw { get; set; }

        public virtual ICollection<DexihTable> DexihTables { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual ICollection<DexihDatalink> DexihDatalinkAuditConnections { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual ICollection<DexihDatajob> DexihDatajobAuditConnections { get; set; }

        public virtual DexihDatabaseType DatabaseType { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public virtual DexihHub Hub { get; set; }

        public string GetPassword(string key)
        {
            if(string.IsNullOrEmpty(PasswordRaw))
            {
                if(string.IsNullOrEmpty(Password))
                {
                    return "";
                }

                if (UsePasswordVariable)
                {
                    return Password;
                }
                
                return EncryptString.Decrypt(Password, key, 1000);
            }
            return PasswordRaw;
        }

        public string GetConnectionString(string key)
        {
            if(string.IsNullOrEmpty(ConnectionStringRaw))
            {
                if(string.IsNullOrEmpty(ConnectionString))
                {
                    return "";
                }

                if (UseConnectionStringVariable)
                {
                    return ConnectionString;
                }
                return EncryptString.Decrypt(ConnectionString, key, 1000);
            }
            return ConnectionStringRaw;
        }

        public bool Encrypt(string key)
        {
            if(!string.IsNullOrEmpty(PasswordRaw))
            {
                Password = EncryptString.Encrypt(PasswordRaw, key, 1000);
                PasswordRaw = null;
            } 
            if(!string.IsNullOrEmpty(PasswordRaw))
            {
                ConnectionString = EncryptString.Encrypt(ConnectionStringRaw, key, 1000);
                ConnectionStringRaw = null;
            }

            return true;
        }

        public Connection GetConnection(string encryptionKey, IEnumerable<DexihHubVariable> hubVariables)
        {
            try
            {
                Type type;
                if (string.IsNullOrEmpty(DatabaseType.Assembly))
                {
                    type = Type.GetType(DatabaseType.Class);
                }
                else
                {

                    var assemblyName = new AssemblyName(DatabaseType.Assembly).Name;
                    var folderPath = Path.GetDirectoryName(this.GetType().GetTypeInfo().Assembly.Location);
                    var assemblyPath = Path.Combine(folderPath, assemblyName + ".dll");
                    if (!File.Exists(assemblyPath))
                    {
                        throw new RepositoryException("The connection could not be started due to a missing assembly.  The assembly name is: " + DatabaseType.Assembly + ", and class: " + DatabaseType.Class + ", and expected in directory: " + this.GetType().GetTypeInfo().Assembly.Location + ".  Have the connections been installed?");
                    }

                    var loader = new AssemblyLoader(folderPath);
                    var assembly = loader.LoadFromAssemblyName(new AssemblyName(assemblyName));
                    type = assembly.GetType(DatabaseType.Class);

                }

                if (type == null)
                {
                    throw new RepositoryException("The connection failed to initialize due to a missing or faulty assembly.  The assembly name is: " + DatabaseType.Assembly + ", and class: " + DatabaseType.Class + ".  Has the connections been installed?");
                }

                var connection = (Connection)Activator.CreateInstance(type);
                this.CopyProperties(connection, true);

                connection.Password = GetPassword(encryptionKey);
                connection.ConnectionString = GetConnectionString(encryptionKey);

                // shift to array to avoid multiple enumerations.
                var hubVariablesArray = hubVariables as DexihHubVariable[] ?? hubVariables.ToArray();
                
                if (hubVariables != null && hubVariablesArray.Any())
                {
                    var hubVariablesManager = new HubVariablesManager(encryptionKey, hubVariablesArray);

                    connection.ConnectionString = hubVariablesManager.InsertHubVariables(connection.ConnectionString);
                    connection.Server = hubVariablesManager.InsertHubVariables(connection.Server);
                    connection.Password = hubVariablesManager.InsertHubVariables(connection.Password);
                    connection.Username = hubVariablesManager.InsertHubVariables(connection.Username);
                    connection.DefaultDatabase = hubVariablesManager.InsertHubVariables(connection.DefaultDatabase);
                    connection.Filename = hubVariablesManager.InsertHubVariables(connection.Filename);
                }

                return connection;
            }
            catch (Exception ex)
            {
                throw new RepositoryException($"Get connection failed.  {ex.Message}", ex);
            }
        }



    }
}

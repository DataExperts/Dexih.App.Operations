﻿using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using Dexih.Utils.Crypto;
using Dexih.Utils.CopyProperties;
using dexih.transforms;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihConnection : DexihHubNamedEntity
    {
        #region Enums

         
        #endregion

        public DexihConnection()
        {
            DexihTables = new HashSet<DexihTable>();
        }


        [Key(7)]
        public string ConnectionAssemblyName { get; set; }

        [Key(8)]
        public string ConnectionClassName { get; set; }

        [Key(9)]
        public EConnectionPurpose Purpose { get; set; }

        [Key(10)]
        public string Server { get; set; }

        [Key(11)]
        public bool UseWindowsAuth { get; set; }

        [Key(12)]
        public string Username { get; set; }

        [Key(13)]
        public string Password { get; set; }

        [Key(14)]
        public bool UsePasswordVariable { get; set; }

        [Key(15)]
        public string DefaultDatabase { get; set; }

        [Key(16)]
        public string Filename { get; set; }

        [Key(17)]
        public bool UseConnectionString { get; set; }

        [Key(18)]
        public string ConnectionString { get; set; }

        [Key(19)]
        public bool UseConnectionStringVariable { get; set; }

        [Key(20)]
        public bool EmbedTableKey { get; set; }

        //these store the raw (unencrypted values) and are not saved to the database.
//        [Key(21)]
//        [NotMapped]
//        public string PasswordRaw { get; set; }

//        [Key(22)]
//        [NotMapped]
//        public string ConnectionStringRaw { get; set; }

        [JsonIgnore, IgnoreMember]
        public ICollection<DexihTable> DexihTables { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public ICollection<DexihDatalink> DexihDatalinkAuditConnections { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public ICollection<DexihDatajob> DexihDatajobAuditConnections { get; set; }


        public override void ResetKeys()
        {
            Key = 0;
        }

        public string GetPassword(string key, int iterations)
        {
//            if(string.IsNullOrEmpty(PasswordRaw))
//            {
//                if(string.IsNullOrEmpty(Password))
//                {
//                    return "";
//                }
//
//                if (UsePasswordVariable)
//                {
//                    return Password;
//                }
                
                return EncryptString.Decrypt(Password, key, iterations);
//            }
//            return PasswordRaw;
        }

        public string GetConnectionString(string key, int iterations)
        {
//            if(string.IsNullOrEmpty(ConnectionStringRaw))
//            {
//                if(string.IsNullOrEmpty(ConnectionString))
//                {
//                    return "";
//                }
//
//                if (UseConnectionStringVariable)
//                {
//                    return ConnectionString;
//                }
                return EncryptString.Decrypt(ConnectionString, key, iterations);
//            }
//            return ConnectionStringRaw;
        }



        public Connection GetConnection(TransformSettings transformSettings)
        {
            try
            {
                var connectionReference = Connections.GetConnection(ConnectionClassName, ConnectionAssemblyName);
                
                if (!transformSettings.RemoteSettings.Permissions.AllowLocalFiles && connectionReference.RequiresLocalStorage)
                {
                    throw new RepositoryException($"The connection {connectionReference.Name} can not be used on this remote agent as local file access is forbidden.");
                }

                var connection = connectionReference.GetConnection();
                this.CopyProperties(connection, true);


                if (!UsePasswordVariable)
                {
                    connection.Password = GetPassword(transformSettings.RemoteSettings.AppSettings.EncryptionKey,
                        transformSettings.RemoteSettings.SystemSettings.EncryptionIterations);
                }

                if (!UseConnectionStringVariable)
                {
                    connection.ConnectionString = GetConnectionString(
                        transformSettings.RemoteSettings.AppSettings.EncryptionKey,
                        transformSettings.RemoteSettings.SystemSettings.EncryptionIterations);
                }

                if (transformSettings.HasVariables())
                {
                    connection.ConnectionString = transformSettings.InsertHubVariables(connection.ConnectionString, true);
                    connection.Server = transformSettings.InsertHubVariables(connection.Server, false);
                    connection.Password = transformSettings.InsertHubVariables(connection.Password, true);
                    connection.Username = transformSettings.InsertHubVariables(connection.Username, false);
                    connection.DefaultDatabase = transformSettings.InsertHubVariables(connection.DefaultDatabase, false);
                    connection.Filename = transformSettings.InsertHubVariables(connection.Filename, false);
                }

                connection.FilePermissions = transformSettings.RemoteSettings.Permissions.GetFilePermissions();

                return connection;
            }
            catch (Exception ex)
            {
                throw new RepositoryException($"Get connection failed.  {ex.Message}", ex);
            }
        }
    }
}

using Microsoft.EntityFrameworkCore;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using System.Linq;
using System;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;
using Dexih.Utils.DataType;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Hosting;
using Newtonsoft.Json;

namespace dexih.repository
{
	public sealed partial class DexihRepositoryContext : IdentityDbContext<ApplicationUser>
	{
		public enum EDatabaseType
		{
			SqlServer, Sqlite, Npgsql, MySql
		}

		public EDatabaseType DatabaseType { get; set; }
        public IHostingEnvironment Environment { get; set; }


		protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
		{
#if DEBUG
            optionsBuilder.EnableSensitiveDataLogging(true);
#endif

			//#warning To protect potentially sensitive information in your connection string, you should move it out of source code. See http://go.microsoft.com/fwlink/?LinkId=723263 for guidance on storing connection strings.
			//optionsBuilder.UseSqlServer(@"Server=(localdb)\v11.0;Database=dexih_repository2;Trusted_Connection=True;");
        }

		public DexihRepositoryContext(DbContextOptions options) : base(options)
		{
		}

        /// <summary>
        /// Adds the hubKey value to all saved entities.
        /// </summary>
        /// <param name="hubKey"></param>
        /// <param name="acceptAllChangesOnSuccess"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task<int> SaveHub(long hubKey, bool acceptAllChangesOnSuccess, CancellationToken cancellationToken = default(CancellationToken))
        {
            var entities = ChangeTracker.Entries().Where(x => x.Entity is DexihHubBaseEntity && (x.State == EntityState.Added || x.State == EntityState.Modified));

            foreach (var entity in entities)
            {
                var property = entity.Entity.GetType().GetProperties().SingleOrDefault(c => c.Name == "HubKey");
                if(property != null)
                {
                    property.SetValue(entity.Entity, hubKey);
                }

            }

            return SaveChangesAsync(acceptAllChangesOnSuccess, cancellationToken);
        }

        public override int SaveChanges()
		{
			AddTimestamps();
			return base.SaveChanges();
		}

		public override Task<int> SaveChangesAsync(bool acceptAllChangesOnSuccess, CancellationToken cancellationToken = default(CancellationToken))
		{
			AddTimestamps();
			return base.SaveChangesAsync(acceptAllChangesOnSuccess, cancellationToken);
		}

		public override int SaveChanges(bool acceptAllChangesOnSuccess)
		{
			AddTimestamps();
			return base.SaveChanges(acceptAllChangesOnSuccess);
		}

		private void AddTimestamps()
        {
			var entities = ChangeTracker.Entries().Where(x => x.Entity is DexihHubBaseEntity && (x.State == EntityState.Added || x.State == EntityState.Modified));

			foreach (var entity in entities)
			{
				if (entity.State == EntityState.Added)
				{
					((DexihHubBaseEntity)entity.Entity).CreateDate = DateTime.UtcNow;
				}
				((DexihHubBaseEntity)entity.Entity).UpdateDate = DateTime.UtcNow;
            }
		}

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            //map the standard identity tables to the repository.
            modelBuilder.Entity<ApplicationUser>().ToTable("dexih_Users");
            modelBuilder.Entity<IdentityRole>().ToTable("dexih_Roles");
            modelBuilder.Entity<IdentityRoleClaim<string>>().ToTable("dexih_RoleClaims");
            modelBuilder.Entity<IdentityUserRole<string>>().ToTable("dexih_UserRoles");
            modelBuilder.Entity<IdentityUserLogin<string>>().ToTable("dexih_UserLogins");
            modelBuilder.Entity<IdentityUserClaim<string>>().ToTable("dexih_UserClaims");
            modelBuilder.Entity<IdentityUserToken<string>>().ToTable("dexih_UserTokens");

            modelBuilder.Entity<DexihColumnValidation>(entity =>
            {
                entity.HasKey(e => e.ColumnValidationKey).HasName("PK_dexih_column_validation");

                entity.ToTable("dexih_column_validation");

                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.ColumnValidationKey).HasColumnName("column_validation_key");

                entity.Property(e => e.AllowDbNull).HasColumnName("allow_db_null");

                entity.Property(e => e.CleanAction).HasColumnName("clean_action").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => (DexihColumnValidation.ECleanAction) Enum.Parse(typeof(DexihColumnValidation.ECleanAction), v));
                
                entity.Property(e => e.InvalidAction).HasColumnName("invalid_action").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => (TransformFunction.EInvalidAction) Enum.Parse(typeof(TransformFunction.EInvalidAction), v));
                
                entity.Property(e => e.Name).IsRequired().HasColumnName("name").HasMaxLength(250);
                entity.Property(e => e.Description).HasColumnName("description").HasMaxLength(1024);

                entity.Property(e => e.DataType).HasColumnName("datatype").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => (DataType.ETypeCode) Enum.Parse(typeof(DataType.ETypeCode), v));
                
                entity.Property(e => e.CleanValue).HasColumnName("clean_value").HasMaxLength(250);

                entity.Property(e => e.ListOfValues).HasColumnName("list_of_values").HasMaxLength(8000)
                    .HasConversion(
                        v => v == null ? null : string.Join("||", v),
                        v => string.IsNullOrEmpty(v) ? null : v.Split(new[] { "||" }, StringSplitOptions.None).ToArray());
                
                entity.Property(e => e.ListOfNotValues).HasColumnName("list_of_not_values").HasMaxLength(8000)
                    .HasConversion(
                        v => v == null ? null : string.Join("||", v),
                        v => string.IsNullOrEmpty(v) ? null : v.Split(new[] { "||" }, StringSplitOptions.None).ToArray());
                

                entity.Property(e => e.LookupColumnKey).HasColumnName("lookup_column_key");
                entity.Property(e => e.LookupIsValid).HasColumnName("lookup_is_valid");
                entity.Property(e => e.LookupMultipleRecords).HasColumnName("lookup_multiple_records");

                entity.Property(e => e.MinLength).HasColumnName("min_length");
                entity.Property(e => e.MaxLength).HasColumnName("max_length");

                entity.Property(e => e.MinValue).HasColumnName("min_value");
                entity.Property(e => e.MaxValue).HasColumnName("max_value");

                entity.Property(e => e.PatternMatch).HasColumnName("pattern_match").HasMaxLength(250);
                entity.Property(e => e.RegexMatch).HasColumnName("regex_match").HasMaxLength(250);

                entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");

                entity.HasOne(d => d.Hub)
                    .WithMany(p => p.DexihColumnValidations)
                    .HasForeignKey(d => d.HubKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_column_validation_dexih_hubs");
                
                entity.HasOne(d => d.LookupColumn)
                    .WithMany(p => p.DexihColumnValidationLookupColumn)
                    .HasForeignKey(d => d.LookupColumnKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_column_validation_lookup_column");
            });

            modelBuilder.Entity<DexihConnection>(entity =>
            {
                entity.HasKey(e => e.ConnectionKey).HasName("PK_dexih_connections");

                entity.ToTable("dexih_connections");

                entity.Property(e => e.HubKey).HasColumnName("hub_key");

                entity.Property(e => e.ConnectionKey).HasColumnName("connection_key");
                entity.Property(e => e.Name).IsRequired().HasColumnName("name").HasMaxLength(50);
                entity.Property(e => e.Description).HasColumnName("description").HasMaxLength(1024);

                entity.Property(e => e.Purpose).HasColumnName("purpose").HasMaxLength(10)
                    .HasConversion(
                        v => v.ToString(),
                        v => (DexihConnection.EConnectionPurpose) Enum.Parse(typeof(DexihConnection.EConnectionPurpose), v));

                entity.Property(e => e.ConnectionAssemblyName).HasColumnName("connection_assembly_name");
                entity.Property(e => e.ConnectionClassName).HasColumnName("connection_class_name");

                entity.Property(e => e.UseConnectionString).HasColumnName("use_connection_string");
                entity.Property(e => e.UseConnectionStringVariable).HasColumnName("use_connection_string_var");
                entity.Property(e => e.ConnectionString).HasColumnName("connection_string").HasMaxLength(1000);

                entity.Property(e => e.Server).HasColumnName("server").HasMaxLength(250);

                entity.Property(e => e.DefaultDatabase).HasColumnName("default_database").HasMaxLength(50);
				entity.Property(e => e.EmbedTableKey).HasColumnName("embed_tablekey");
                entity.Property(e => e.Filename).HasColumnName("filename").HasMaxLength(1000);
//                entity.Property(e => e.IsInternal).HasColumnName("is_internal");

				entity.Property(e => e.UseWindowsAuth).HasColumnName("use_windows_auth");
                entity.Property(e => e.Username).HasColumnName("username").HasMaxLength(250);
                entity.Property(e => e.Password).HasColumnName("password").HasMaxLength(250);
                entity.Property(e => e.UsePasswordVariable).HasColumnName("use_password_var");

                entity.Property(e => e.CreateDate).HasColumnName("create_date");
				entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");

                entity.HasOne(d => d.Hub)
                    .WithMany(p => p.DexihConnections)
                    .HasForeignKey(d => d.HubKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_connections_dexih_hubs");
            });
            
            modelBuilder.Entity<DexihCustomFunction>(entity =>
            {
                entity.HasKey(e => e.CustomFunctionKey).HasName("PK_dexih_custom_functions");

                entity.ToTable("dexih_custom_functions");

                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.CustomFunctionKey).HasColumnName("custom_function_key");
                entity.Property(e => e.MethodCode).HasColumnName("method_code").HasMaxLength(8000);
                entity.Property(e => e.ResultCode).HasColumnName("result_code").HasMaxLength(8000);
                entity.Property(e => e.Name).HasColumnName("name").IsRequired().HasMaxLength(50);
                entity.Property(e => e.Description).HasColumnName("description").HasMaxLength(1024);

                entity.Property(e => e.IsGeneric).HasColumnName("is_generic");
                entity.Property(e => e.GenericTypeDefault).HasColumnName("generic_type_default").HasMaxLength(20).HasConversion(
                    v => v.ToString(),
                    v => (DataType.ETypeCode)Enum.Parse(typeof(DataType.ETypeCode), v));

                entity.Property(e => e.FunctionType).HasColumnName("function_type").HasMaxLength(30)
                    .HasConversion(
                        v => v.ToString(),
                        v => (EFunctionType) Enum.Parse(typeof(EFunctionType), v));
                
                entity.Property(e => e.ReturnType).HasColumnName("return_type").HasMaxLength(30)
                    .HasConversion(
                        v => v.ToString(),
                        v => (DataType.ETypeCode) Enum.Parse(typeof(DataType.ETypeCode), v));

                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");
            });
            
            modelBuilder.Entity<DexihCustomFunctionParameter>(entity =>
            {
                entity.HasKey(e => e.CustomFunctionParameterKey).HasName("PK_dexih_cust_function_parameters");

                entity.ToTable("dexih_custom_function_parameters");

                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.CustomFunctionParameterKey).HasColumnName("custom_function_parameter_key");
                entity.Property(e => e.CustomFunctionKey).HasColumnName("custom_function_key");
                entity.Property(e => e.ParameterName).IsRequired().HasColumnName("parameter_name").HasMaxLength(50);
                // entity.Property(e => e.Name).IsRequired().HasColumnName("name").HasMaxLength(50);
                entity.Property(e => e.Description).HasColumnName("description").HasMaxLength(250);
                entity.Property(e => e.DataType).HasColumnName("datatype").HasMaxLength(20)
                    .HasConversion(
                        v => v.ToString(),
                        v => (DataType.ETypeCode) Enum.Parse(typeof(DataType.ETypeCode), v));
                entity.Property(e => e.IsGeneric).HasColumnName("is_generic");
                entity.Property(e => e.Direction).HasColumnName("direction").HasMaxLength(20)
                    .HasConversion(
                        v => v.ToString(),
                        v => (DexihParameterBase.EParameterDirection) Enum.Parse(typeof(DexihParameterBase.EParameterDirection), v));

                entity.Property(e => e.Position).HasColumnName("position");
                entity.Property(e => e.Rank).HasColumnName("rank");

                entity.Property(e => e.CreateDate).HasColumnName("create_date");
				entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");

                entity.HasOne(d => d.CustomFunction)
                    .WithMany(p => p.DexihCustomFunctionParameters)
                    .HasForeignKey(d => d.CustomFunctionKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_customfunction_customfunction_parameter");
                

            });

            modelBuilder.Entity<DexihDatajob>(entity =>
            {
                entity.HasKey(e => e.DatajobKey).HasName("PK_dexih_datajob");

                entity.ToTable("dexih_datajobs");

                entity.Property(e => e.DatajobKey).HasColumnName("datajob_key");
                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.Name).IsRequired().HasColumnName("name").HasMaxLength(50);
                entity.Property(e => e.Description).HasColumnName("description").HasMaxLength(1024);

                entity.Property(e => e.FailAction).HasColumnName("fail_action").HasMaxLength(20)
                    .HasConversion(
                        v => v.ToString(),
                        v => (DexihDatajob.EFailAction) Enum.Parse(typeof(DexihDatajob.EFailAction), v));

                entity.Property(e => e.AuditConnectionKey).HasColumnName("audit_connection_key");
                // entity.Property(e => e.ExternalTrigger).HasColumnName("external_trigger");
                entity.Property(e => e.FileWatch).HasColumnName("file_watch");

                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");

                entity.HasOne(d => d.Hub)
                    .WithMany(p => p.DexihDatajobs)
                    .HasForeignKey(d => d.HubKey)
                    .HasConstraintName("FK_dexih_datajobs_dexih_hubs");

                entity.HasOne(d => d.AuditConnection)
                    .WithMany(p => p.DexihDatajobAuditConnections)
                    .HasForeignKey(d => d.AuditConnectionKey)
                    .HasConstraintName("FK_dexih_datajobs_audit_connection");
            });

            modelBuilder.Entity<DexihDatalinkColumn>(entity =>
            {
                entity.HasKey(e => e.DatalinkColumnKey).HasName("PK_dexih_datalink_columns");

                entity.ToTable("dexih_datalink_columns");

                entity.Property(e => e.DatalinkColumnKey).HasColumnName("datalink_column_key");
                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.DatalinkTableKey).HasColumnName("datalink_table_key");
                entity.Property(e => e.ParentDatalinkColumnKey).HasColumnName("parent_datalink_column_key");
                entity.Property(e => e.AllowDbNull).HasColumnName("allow_db_null");
                entity.Property(e => e.Name).IsRequired().HasColumnName("name").HasMaxLength(250);
                entity.Property(e => e.DataType).HasColumnName("datatype").HasMaxLength(20)
                    .HasConversion(
                        v => v.ToString(),
                        v => (DataType.ETypeCode) Enum.Parse(typeof(DataType.ETypeCode), v));

                entity.Property(e => e.DeltaType).HasColumnName("delta_type").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => (TableColumn.EDeltaType) Enum.Parse(typeof(TableColumn.EDeltaType), v));
                
                entity.Property(e => e.Description).HasColumnName("description").HasMaxLength(1024);
                entity.Property(e => e.IsIncrementalUpdate).HasColumnName("is_incremental_update");
                entity.Property(e => e.IsMandatory).HasColumnName("is_mandatory");
                entity.Property(e => e.IsUnique).HasColumnName("is_unique");
                entity.Property(e => e.LogicalName).HasColumnName("logical_name").HasMaxLength(250);   
                entity.Property(e => e.ColumnGroup).HasColumnName("column_group").HasMaxLength(250);
                entity.Property(e => e.DefaultValue).HasColumnName("default_value").HasMaxLength(1024);
                entity.Property(e => e.IsUnicode).HasColumnName("is_unicode");
                entity.Property(e => e.MaxLength).HasColumnName("max_length");
                entity.Property(e => e.Position).HasColumnName("position");
                entity.Property(e => e.Precision).HasColumnName("precision");
                entity.Property(e => e.Scale).HasColumnName("scale");
                entity.Property(e => e.SecurityFlag).HasColumnName("security_flag").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => (TableColumn.ESecurityFlag) Enum.Parse(typeof(TableColumn.ESecurityFlag), v));

                entity.Property(e => e.IsInput).HasColumnName("is_input");
                entity.Property(e => e.Rank).HasColumnName("rank");

                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");

                entity.HasOne(d => d.DatalinkTable)
                    .WithMany(p => p.DexihDatalinkColumns)
                    .HasForeignKey(d => d.DatalinkTableKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalinktable_columns_dexih_datalinktables");
                
                entity.HasOne(d => d.ParentColumn)
                    .WithMany(p => p.ChildColumns)
                    .HasForeignKey(d => d.ParentDatalinkColumnKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_table_child_columns");

            });

            modelBuilder.Entity<DexihDatalinkTable>(entity =>
            {
                entity.HasKey(e => e.DatalinkTableKey).HasName("PK_dexih_datalink_table");

                entity.ToTable("dexih_datalink_table");

                entity.Property(e => e.DatalinkTableKey).HasColumnName("datalink_table_key");
                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.Name).HasColumnName("name").HasMaxLength(50);
                entity.Property(e => e.SourceTableKey).HasColumnName("source_table_key");
                entity.Property(e => e.SourceDatalinkKey).HasColumnName("source_datalink_key");
                entity.Property(e => e.SourceType).HasColumnName("source_type").HasMaxLength(20)
                    .HasConversion(
                        v => v.ToString(),
                        v => (ESourceType) Enum.Parse(typeof(ESourceType), v));

                entity.Property(e => e.RowsStartAt).HasColumnName("rows_start_at");
                entity.Property(e => e.RowsEndAt).HasColumnName("rows_end_at");
                entity.Property(e => e.RowsIncrement).HasColumnName("rows_increment");

                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");
                
                entity.HasOne(d => d.SourceTable)
                    .WithMany(p => p.DexihDatalinkTables)
                    .HasForeignKey(d => d.SourceTableKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_source_tables");

                entity.HasOne(d => d.SourceDatalink)
                    .WithMany(p => p.DexihDatalinkTables)
                    .HasForeignKey(d => d.SourceDatalinkKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_source_datalinks");

            });

            modelBuilder.Entity<DexihDatalinkDependency>(entity =>
            {
                entity.HasKey(e => e.DatalinkDependencyKey)
                    .HasName("PK_dexih_datalink_dependencies");

                entity.ToTable("dexih_datalink_dependencies");

                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.DatalinkDependencyKey).HasColumnName("datalink_dependency_key");
                entity.Property(e => e.DatalinkStepKey).HasColumnName("datalink_step_key");
                entity.Property(e => e.DependentDatalinkStepKey).HasColumnName("dependent_datalink_step_key");

                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");

                entity.HasOne(d => d.DependentDatalinkStep)
                    .WithMany(p => p.DexihDatalinkDependentSteps)
                    .HasForeignKey(d => d.DependentDatalinkStepKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_deps_dl_dep");
                
                entity.HasOne(d => d.DatalinkStep)
                    .WithMany(p => p.DexihDatalinkDependencies)
                    .HasForeignKey(d => d.DatalinkStepKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_deps_dl_step");
            });

            modelBuilder.Entity<DexihDatalinkProfile>(entity =>
            {
                entity.HasKey(e => e.DatalinkProfileKey)
                    .HasName("PK_dexih_datalink_profile_rules");

                entity.ToTable("dexih_datalink_profiles");

                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.DatalinkProfileKey).HasColumnName("datalink_profile_key");
                entity.Property(e => e.DatalinkKey).HasColumnName("datalink_key");
				entity.Property(e => e.DetailedResults).HasColumnName("detailed_results");

                entity.Property(e => e.FunctionClassName).HasColumnName("function_class_name");
                entity.Property(e => e.FunctionMethodName).HasColumnName("function_method_name");
                entity.Property(e => e.FunctionAssemblyName).HasColumnName("function_assembly_name");

                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");

                entity.HasOne(d => d.Datalink)
                    .WithMany(p => p.DexihDatalinkProfiles)
                    .HasForeignKey(d => d.DatalinkKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_profiles_dexih_datalinks");

            });

 

            modelBuilder.Entity<DexihDatalinkStep>(entity =>
            {
                entity.HasKey(e => e.DatalinkStepKey)
                    .HasName("PK_dexih_datajobs_datalinks");

                entity.ToTable("dexih_datalink_step");

                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.DatalinkStepKey).HasColumnName("datalink_step_key");
                entity.Property(e => e.Name).HasColumnName("name").HasMaxLength(50);

                entity.Property(e => e.DatajobKey).HasColumnName("datajob_key");
                entity.Property(e => e.DatalinkKey).HasColumnName("datalink_key");

				entity.Property(e => e.CreateDate).HasColumnName("create_date");
				entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");

                entity.HasOne(d => d.Datajob)
                    .WithMany(p => p.DexihDatalinkSteps)
                    .HasForeignKey(d => d.DatajobKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datajobs_datalinks_dexih_datajobs");

                entity.HasOne(d => d.Datalink)
                    .WithMany(p => p.DexihDatalinkSteps)
                    .HasForeignKey(d => d.DatalinkKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_step_dexih_datalinks");
            });            
            
            modelBuilder.Entity<DexihDatalinkStepColumn>(entity =>
            {
                entity.HasKey(e => e.DatalinkStepColumnKey).HasName("PK_dexih_datalink_step_columns");

                entity.ToTable("dexih_datalink_step_columns");

                entity.Property(e => e.DatalinkStepColumnKey).HasColumnName("datalink_step_column_key");
                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.DatalinkStepKey).HasColumnName("datalink_step_key");
                entity.Property(e => e.AllowDbNull).HasColumnName("allow_db_null");
                entity.Property(e => e.Name).IsRequired().HasColumnName("name").HasMaxLength(250);
                entity.Property(e => e.DataType).HasColumnName("datatype").HasMaxLength(20)
                    .HasConversion(
                        v => v.ToString(),
                        v => (DataType.ETypeCode) Enum.Parse(typeof(DataType.ETypeCode), v));
                entity.Property(e => e.DeltaType).HasColumnName("delta_type").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => (TableColumn.EDeltaType) Enum.Parse(typeof(TableColumn.EDeltaType), v));
                entity.Property(e => e.Description).HasColumnName("description").HasMaxLength(1024);
                entity.Property(e => e.IsIncrementalUpdate).HasColumnName("is_incremental_update");
                entity.Property(e => e.IsMandatory).HasColumnName("is_mandatory");
                entity.Property(e => e.IsUnique).HasColumnName("is_unique");
                entity.Property(e => e.LogicalName).HasColumnName("logical_name").HasMaxLength(250);    
                entity.Property(e => e.ColumnGroup).HasColumnName("column_group").HasMaxLength(250);
                entity.Property(e => e.DefaultValue).HasColumnName("default_value").HasMaxLength(1024);
                entity.Property(e => e.IsUnicode).HasColumnName("is_unicode");
                entity.Property(e => e.MaxLength).HasColumnName("max_length");
                entity.Property(e => e.Position).HasColumnName("position");
                entity.Property(e => e.Precision).HasColumnName("precision");
                entity.Property(e => e.Scale).HasColumnName("scale");
                entity.Property(e => e.SecurityFlag).HasColumnName("security_flag").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => (TableColumn.ESecurityFlag) Enum.Parse(typeof(TableColumn.ESecurityFlag), v));

                entity.Property(e => e.IsInput).HasColumnName("is_input");
                entity.Property(e => e.Rank).HasColumnName("rank");
                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");

                entity.HasOne(d => d.DatalinkStep)
                    .WithMany(p => p.DexihDatalinkStepColumns)
                    .HasForeignKey(d => d.DatalinkStepKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalinkstep_columns_dexih_datalinksteps");

            });
            
            modelBuilder.Entity<DexihDatalinkTarget>(entity =>
            {
                entity.HasKey(e => e.DatalinkTargetKey).HasName("PK_dexih_datalink_target");

                entity.ToTable("dexih_datalink_target");

                entity.Property(e => e.DatalinkTargetKey).HasColumnName("datalink_target_key");
                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.DatalinkKey).HasColumnName("datalink_key");
                entity.Property(e => e.Position).HasColumnName("position");
                entity.Property(e => e.NodeDatalinkColumnKey).HasColumnName("node_datalink_column_key");
                entity.Property(e => e.TableKey).HasColumnName("table_key");
                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");
                
                entity.HasOne(d => d.Datalink)
                    .WithMany(p => p.DexihDatalinkTargets)
                    .HasForeignKey(d => d.DatalinkKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_target_datalink");
                
                entity.HasOne(d => d.NodeDatalinkColumn)
                    .WithMany(p => p.DexihDatalinkTargetNodeColumn)
                    .HasForeignKey(d => d.NodeDatalinkColumnKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_target_datalink_node_column");
                
                entity.HasOne(d => d.Table)
                    .WithMany(p => p.DexihTargetTables)
                    .HasForeignKey(d => d.TableKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_target_table");
                
            });
            
            modelBuilder.Entity<DexihDatalinkTest>(entity =>
            {
                entity.HasKey(e => e.DatalinkTestKey).HasName("PK_dexih_datalink_tests");

                entity.ToTable("dexih_datalink_tests");

                entity.Property(e => e.DatalinkTestKey).HasColumnName("datalink_test_key");
                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.Name).HasColumnName("name").HasMaxLength(50);
                entity.Property(e => e.Description).HasColumnName("description").HasMaxLength(1000);
                entity.Property(e => e.AuditConnectionKey).HasColumnName("audit_connection_key");
                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");
            });
            
            modelBuilder.Entity<DexihDatalinkTestStep>(entity =>
            {
                entity.HasKey(e => e.DatalinkTestStepKey).HasName("PK_dexih_datalink_test_steps");

                entity.ToTable("dexih_datalink_test_steps");

                entity.Property(e => e.DatalinkTestStepKey).HasColumnName("datalink_test_step_key");
                entity.Property(e => e.DatalinkTestKey).HasColumnName("datalink_test_key");
                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.Position).HasColumnName("position");
                entity.Property(e => e.DatalinkKey).HasColumnName("datalink_key");
                
                entity.Property(e => e.Name).HasColumnName("name").HasMaxLength(50);
                entity.Property(e => e.Description).HasColumnName("description").HasMaxLength(1000);
                
                entity.Property(e => e.TargetConnectionKey).HasColumnName("target_connection_key");
                entity.Property(e => e.TargetTableName).HasColumnName("target_table_name").HasMaxLength(50);
                entity.Property(e => e.TargetSchema).HasColumnName("target_schema").HasMaxLength(50);

                entity.Property(e => e.ExpectedConnectionKey).HasColumnName("expected_connection_key");
                entity.Property(e => e.ExpectedTableName).HasColumnName("expected_table_name").HasMaxLength(50);
                entity.Property(e => e.ExpectedSchema).HasColumnName("expected_schema").HasMaxLength(50);

                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");

                entity.HasOne(d => d.DatalinkTest)
                    .WithMany(p => p.DexihDatalinkTestSteps)
                    .HasForeignKey(d => d.DatalinkTestKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_test_datalink_test_step");

            });
            
            modelBuilder.Entity<DexihDatalinkTestTable>(entity =>
            {
                entity.HasKey(e => e.DatalinkTestTableKey).HasName("PK_dexih_datalink_test_tables");

                entity.ToTable("dexih_datalink_test_tables");

                entity.Property(e => e.DatalinkTestTableKey).HasColumnName("datalink_test_table_key");
                entity.Property(e => e.DatalinkTestStepKey).HasColumnName("datalink_test_step_key");
                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                
                entity.Property(e => e.TableKey).HasColumnName("table_key");
                entity.Property(e => e.SourceConnectionKey).HasColumnName("source_connection_key");
                entity.Property(e => e.SourceTableName).HasColumnName("source_table_name").HasMaxLength(50);
                entity.Property(e => e.SourceSchema).HasColumnName("source_schema").HasMaxLength(50);

                entity.Property(e => e.TestConnectionKey).HasColumnName("test_connection_key");
                entity.Property(e => e.TestTableName).HasColumnName("test_table_name").HasMaxLength(50);
                entity.Property(e => e.TestSchema).HasColumnName("test_schema").HasMaxLength(50);

                entity.Property(e => e.Action).HasColumnName("action").HasMaxLength(20)
                    .HasConversion(
                    v => v.ToString(),
                    v => (DexihDatalinkTestTable.ETestTableAction) Enum.Parse(typeof(DexihDatalinkTestTable.ETestTableAction), v));

                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");
                
                entity.HasOne(d => d.DatalinkTestStep)
                    .WithMany(p => p.DexihDatalinkTestTables)
                    .HasForeignKey(d => d.DatalinkTestStepKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_test_step_datalink_test_table");
            });

            modelBuilder.Entity<DexihDatalinkTransformItem>(entity =>
            {
                entity.HasKey(e => e.DatalinkTransformItemKey).HasName("PK_dexih_datalink_transform_items");
                entity.ToTable("dexih_datalink_transform_items");

                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.DatalinkTransformItemKey).HasColumnName("datalink_transform_item_key");
                entity.Property(e => e.DatalinkTransformKey).HasColumnName("datalink_transform_key");

                entity.Property(e => e.FunctionCode).HasColumnName("function_code").HasMaxLength(8000);
                entity.Property(e => e.FunctionResultCode).HasColumnName("function_result_code").HasMaxLength(8000);
                entity.Property(e => e.InvalidAction).HasColumnName("invalid_action").HasMaxLength(20)
                    .HasConversion(
                        v => v.ToString(),
                        v => (TransformFunction.EInvalidAction) Enum.Parse(typeof(TransformFunction.EInvalidAction), v));

                entity.Property(e => e.JoinDatalinkColumnKey).HasColumnName("join_datalink_column_key");
                entity.Property(e => e.FilterDatalinkColumnKey).HasColumnName("filter_datalink_column_key");
                entity.Property(e => e.SourceValue).HasColumnName("source_value").HasMaxLength(1000);
                entity.Property(e => e.JoinValue).HasColumnName("join_value").HasMaxLength(1000);
                entity.Property(e => e.FilterValue).HasColumnName("filter_value").HasMaxLength(1000);
                entity.Property(e => e.FilterCompare).HasColumnName("filter_compare").HasMaxLength(50)
                    .HasConversion(
                        v => v == null ? null : v.ToString(),
                        v => String.IsNullOrEmpty(v) ? null : (Filter.ECompare?) Enum.Parse(typeof(Filter.ECompare), v));
                entity.Property(e => e.Aggregate).HasColumnName("aggregate").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => String.IsNullOrEmpty(v) ? SelectColumn.EAggregate.Sum : (SelectColumn.EAggregate) Enum.Parse(typeof(SelectColumn.EAggregate), v));
                entity.Property(e => e.OnError).HasColumnName("on_error").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => String.IsNullOrEmpty(v) ? EErrorAction.Abend : (EErrorAction) Enum.Parse(typeof(EErrorAction), v));
                entity.Property(e => e.OnNull).HasColumnName("on_null").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => String.IsNullOrEmpty(v) ? EErrorAction.Abend : (EErrorAction) Enum.Parse(typeof(EErrorAction), v));
                entity.Property(e => e.SortDirection).HasColumnName("sort_direction").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => String.IsNullOrEmpty(v) ? Sort.EDirection.Descending : (Sort.EDirection) Enum.Parse(typeof(Sort.EDirection), v));
                entity.Property(e => e.SeriesGrain).HasColumnName("series_grain").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => (ESeriesGrain) Enum.Parse(typeof(ESeriesGrain), v));
                entity.Property(e => e.TransformItemType).HasColumnName("transform_item_type").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => (DexihDatalinkTransformItem.ETransformItemType) Enum.Parse(typeof(DexihDatalinkTransformItem.ETransformItemType), v));

                entity.Property(e => e.NotCondition).HasColumnName("not_condition");
                entity.Property(e => e.Position).HasColumnName("position");
                entity.Property(e => e.SeriesFill).HasColumnName("series_fill");
                entity.Property(e => e.SeriesStart).HasColumnName("series_start").HasMaxLength(40);
                entity.Property(e => e.SeriesFinish).HasColumnName("series_finish").HasMaxLength(40);
                entity.Property(e => e.SourceDatalinkColumnKey).HasColumnName("source_datalink_column_key");
                entity.Property(e => e.FunctionClassName).HasColumnName("function_class_name");
                entity.Property(e => e.FunctionMethodName).HasColumnName("function_method_name");
                entity.Property(e => e.FunctionAssemblyName).HasColumnName("function_assembly_name");
                entity.Property(e => e.FunctionCaching).HasColumnName("function_caching").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => (MapFunction.EFunctionCaching) Enum.Parse(typeof(MapFunction.EFunctionCaching), v));

                entity.Property(e => e.IsGeneric).HasColumnName("is_generic");
                entity.Property(e => e.GenericTypeCode).HasColumnName("generic_type_code").HasMaxLength(20)
                .HasConversion(
                    v => v == null ? null : v.ToString(),
                    v => string.IsNullOrEmpty(v) ? (DataType.ETypeCode?) null : Enum.Parse<DataType.ETypeCode>(v)
                );

                entity.Property(e => e.CustomFunctionKey).HasColumnName("custom_function_key");
                entity.Property(e => e.TargetDatalinkColumnKey).HasColumnName("target_datalink_column_key");

                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");

                entity.HasOne(d => d.Dt)
                    .WithMany(p => p.DexihDatalinkTransformItems)
                    .HasForeignKey(d => d.DatalinkTransformKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_transform_items_dexih_datalink_transforms");

                entity.HasOne(d => d.SourceDatalinkColumn)
                    .WithMany(p => p.DexihDatalinkTransformItemsSourceColumn)
                    .HasForeignKey(d => d.SourceDatalinkColumnKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_transform_items_source_column");

                entity.HasOne(d => d.TargetDatalinkColumn)
                    .WithMany(p => p.DexihDatalinkTransformItemsTargetColumn)
                    .HasForeignKey(d => d.TargetDatalinkColumnKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_transform_items_target_column");

                entity.HasOne(d => d.JoinDatalinkColumn)
                    .WithMany(p => p.DexihDatalinkTransformItemsJoinColumn)
                    .HasForeignKey(d => d.JoinDatalinkColumnKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_transform_items_join_column");

                entity.HasOne(d => d.FilterDatalinkColumn)
                    .WithMany(p => p.DexihDatalinkTransformItemsFilterColumn)
                    .HasForeignKey(d => d.FilterDatalinkColumnKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_transform_items_filter_column");

                entity.HasOne(d => d.CustomFunction)
                    .WithMany(p => p.DexihDatalinkTransformItemCustomFunction)
                    .HasForeignKey(d => d.CustomFunctionKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_transform_items_dexih_custom_functions");

            });

            modelBuilder.Entity<DexihDatalinkTransform>(entity =>
            {
                entity.HasKey(e => e.DatalinkTransformKey).HasName("PK_dexih_datalink_transforms");
                entity.ToTable("dexih_datalink_transforms");

                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.DatalinkTransformKey).HasColumnName("datalink_transform_key");
                entity.Property(e => e.DatalinkKey).HasColumnName("datalink_key");
                entity.Property(e => e.Position).HasColumnName("position");

                entity.Property(e => e.TransformType).HasColumnName("transform_type").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => (TransformAttribute.ETransformType) Enum.Parse(typeof(TransformAttribute.ETransformType), v));

                entity.Property(e => e.TransformClassName).HasColumnName("transform_class_name").HasMaxLength(250);
                entity.Property(e => e.TransformAssemblyName).HasColumnName("transform_assembly_name").HasMaxLength(250);

                entity.Property(e => e.Name).HasColumnName("name").HasMaxLength(250);
                entity.Property(e => e.Description).HasColumnName("description").HasMaxLength(1024);

                entity.Property(e => e.PassThroughColumns).HasColumnName("pass_through_columns");

                entity.Property(e => e.JoinDatalinkTableKey).HasColumnName("join_datalink_table_key");
                
                entity.Property(e => e.NodeDatalinkColumnKey).HasColumnName("node_datalink_column_key");

                entity.Property(e => e.JoinDuplicateStrategy).HasColumnName("join_duplicate_strategy").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => (Transform.EDuplicateStrategy) Enum.Parse(typeof(Transform.EDuplicateStrategy), v));

                entity.Property(e => e.JoinSortDatalinkColumnKey).HasColumnName("join_sort_column_key");

                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");


                entity.HasOne(d => d.Datalink)
                    .WithMany(p => p.DexihDatalinkTransforms)
                    .HasForeignKey(d => d.DatalinkKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_transforms_dexih_datalinks");

                entity.HasOne(d => d.JoinDatalinkTable)
                    .WithMany(p => p.DexihDatalinkTransforms)
                    .HasForeignKey(d => d.JoinDatalinkTableKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_transforms_datalink_join_table");

                entity.HasOne(d => d.JoinSortDatalinkColumn)
                    .WithMany(p => p.DexihDatalinkTransformsJoinSortColumn)
                    .HasForeignKey(d => d.JoinSortDatalinkColumnKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_transforms_datalink_join_sort_column");
                
                entity.HasOne(d => d.NodeDatalinkColumn)
                    .WithMany(p => p.DexihDatalinkTransformsNodeColumn)
                    .HasForeignKey(d => d.NodeDatalinkColumnKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_transforms_datalink_node_column");
            });

            modelBuilder.Entity<DexihDatalink>(entity =>
            {
                entity.HasKey(e => e.DatalinkKey).HasName("PK_dexih_datalink");
                entity.ToTable("dexih_datalinks");

                entity.Property(e => e.DatalinkKey).HasColumnName("datalink_key");
                entity.Property(e => e.HubKey).HasColumnName("hub_key");

                entity.Property(e => e.Name).HasColumnName("name").HasMaxLength(250);
                entity.Property(e => e.Description).HasColumnName("description").HasMaxLength(1024);

                entity.Property(e => e.DatalinkType).HasColumnName("datalink_type").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => (DexihDatalink.EDatalinkType) Enum.Parse(typeof(DexihDatalink.EDatalinkType), v));
                
                entity.Property(e => e.SourceDatalinkTableKey).HasColumnName("source_datalink_table_key");
                // entity.Property(e => e.TargetTableKey).HasColumnName("target_table_key");
                entity.Property(e => e.AuditConnectionKey).HasColumnName("audit_connection_key");

                entity.Property(e => e.MaxRows).HasColumnName("max_rows");
                entity.Property(e => e.AddDefaultRow).HasColumnName("add_default_row");
                entity.Property(e => e.IsQuery).HasColumnName("is_query");
                entity.Property(e => e.RollbackOnFail).HasColumnName("rollback_on_fail");
                entity.Property(e => e.RowsPerCommit).HasColumnName("rows_per_commit");
                entity.Property(e => e.RowsPerProgress).HasColumnName("rows_per_progress");

                entity.Property(e => e.LoadStrategy).HasColumnName("load_strategy").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => (TransformWriterTarget.ETransformWriterMethod) Enum.Parse(typeof(TransformWriterTarget.ETransformWriterMethod), v));
                
                entity.Property(e => e.UpdateStrategy).HasColumnName("update_strategy").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => (TransformDelta.EUpdateStrategy) Enum.Parse(typeof(TransformDelta.EUpdateStrategy), v));
                
                entity.Property(e => e.ProfileTableName).HasColumnName("profile_table_name");
                entity.Property(e => e.IsShared).HasColumnName("is_shared");

                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");

                entity.HasOne(d => d.SourceDatalinkTable)
                    .WithMany(p => p.DexihDatalinkSourceTables)
                    .HasForeignKey(d => d.SourceDatalinkTableKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_source_tables");

                entity.HasOne(d => d.Hub)
                    .WithMany(p => p.DexihDatalinks)
                    .HasForeignKey(d => d.HubKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalinks_dexih_hubs");

//                entity.HasOne(d => d.TargetTable)
//                    .WithMany(p => p.DexihTargetTables)
//                    .HasForeignKey(d => d.TargetTableKey)
//                    .OnDelete(DeleteBehavior.Restrict)
//                    .HasConstraintName("FK_dexih_target_tables");

                entity.HasOne(d => d.AuditConnection)
                    .WithMany(p => p.DexihDatalinkAuditConnections)
                    .HasForeignKey(d => d.AuditConnectionKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalinks_audit_connection");
            });

            modelBuilder.Entity<DexihFileFormat>(entity =>
            {
                entity.HasKey(e => e.FileFormatKey)
                    .HasName("PK_dexih_file_format");

                entity.ToTable("dexih_file_format");

                entity.Property(e => e.FileFormatKey).HasColumnName("file_format_key");
                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.Name).HasColumnName("name").HasMaxLength(50);
                entity.Property(e => e.Description).HasColumnName("description").HasMaxLength(1024);
                
                entity.Property(e => e.MatchHeaderRecord).HasColumnName("match_header_record");
                entity.Property(e => e.SkipHeaderRows).HasColumnName("skip_header_rows");
                entity.Property(e => e.AllowComments).HasColumnName("allow_comments");
                entity.Property(e => e.BufferSize).HasColumnName("buffer_size");
                entity.Property(e => e.Comment).HasColumnName("comment").HasColumnType("char(1)");
                entity.Property(e => e.Delimiter).HasColumnName("delimiter");
                entity.Property(e => e.DetectColumnCountChanges).HasColumnName("detect_column_count_changes");
                entity.Property(e => e.HasHeaderRecord).HasColumnName("has_header_record");
                entity.Property(e => e.IgnoreHeaderWhiteSpace).HasColumnName("ignore_header_white_space");
                entity.Property(e => e.IgnoreReadingExceptions).HasColumnName("ignore_reading_exceptions");
                entity.Property(e => e.IgnoreQuotes).HasColumnName("ignore_quotes");
                entity.Property(e => e.Quote).HasColumnName("quote").HasColumnType("char(1)");
                entity.Property(e => e.QuoteAllFields).HasColumnName("quote_all_fields");
                entity.Property(e => e.QuoteNoFields).HasColumnName("quote_no_fields");
                entity.Property(e => e.SkipEmptyRecords).HasColumnName("skip_empty_records");
                entity.Property(e => e.TrimFields).HasColumnName("trim_fields");
                entity.Property(e => e.TrimHeaders).HasColumnName("trim_headers");
                entity.Property(e => e.WillThrowOnMissingField).HasColumnName("will_throw_on_missing_field");
                entity.Property(e => e.SetWhiteSpaceCellsToNull).HasColumnName("set_white_space_cells_to_null");

                entity.Property(e => e.IsValid).HasColumnName("is_valid");
                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.UpdateDate).HasColumnName("update_date");

				entity.HasOne(d => d.Hub)
					  .WithMany(p => p.DexihFileFormats)
                    .HasForeignKey(d => d.HubKey);
            });

            modelBuilder.Entity<DexihFunctionParameter>(entity =>
            {
                entity.HasKey(e => e.FunctionParameterKey)
                    .HasName("PK_dexih_function_parameters");

                entity.ToTable("dexih_function_parameters");

                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.FunctionParameterKey).HasColumnName("function_parameter_key");

                entity.Property(e => e.DatalinkColumnKey).HasColumnName("datalink_column_key");
                entity.Property(e => e.DatalinkTransformItemKey).HasColumnName("datalink_transform_item_key");
                
                entity.Property(e => e.DataType).HasColumnName("datatype").HasMaxLength(20)
                    .HasConversion(
                        v => v.ToString(),
                        v => (DataType.ETypeCode) Enum.Parse(typeof(DataType.ETypeCode), v));

                entity.Property(e => e.IsGeneric).HasColumnName("is_generic");

                entity.Property(e => e.Direction).HasColumnName("direction").HasMaxLength(20)
                    .HasConversion(
                        v => v.ToString(),
                        v => (DexihParameterBase.EParameterDirection) Enum.Parse(typeof(DexihParameterBase.EParameterDirection), v));
                
                entity.Property(e => e.ParameterName).IsRequired().HasColumnName("parameter_name").HasMaxLength(50);
                entity.Property(e => e.Value).HasColumnName("value").HasMaxLength(1024);
                entity.Property(e => e.Position).HasColumnName("position");
                entity.Property(e => e.Rank).HasColumnName("rank");
                entity.Property(e => e.ListOfValues).HasColumnName("list_of_values").HasMaxLength(8000)
                    .HasConversion(
                        v => v == null ? null : string.Join("||", v),
                        v => string.IsNullOrEmpty(v) ? null : v.Split(new[] { "||" }, StringSplitOptions.None).ToArray());
                
                entity.Property(e => e.CreateDate).HasColumnName("create_date");
				entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");

                entity.HasOne(d => d.DtItem)
                    .WithMany(p => p.DexihFunctionParameters)
                    .HasForeignKey(d => d.DatalinkTransformItemKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_function_parameters_dexih_datalink_transform_items");

                entity.HasOne(d => d.DatalinkColumn)
                    .WithMany(p => p.DexihFunctionParameterColumn)
                    .HasForeignKey(d => d.DatalinkColumnKey)
                    .HasConstraintName("FK_dexih_function_parameters_dexih_table_columns");
            });
            
            modelBuilder.Entity<DexihFunctionArrayParameter>(entity =>
            {
                entity.HasKey(e => e.FunctionArrayParameterKey).HasName("PK_dexih_function_array_param_key");

                entity.ToTable("dexih_function_array_parameters");

                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.FunctionArrayParameterKey).HasColumnName("function_parameter_array_key");
                entity.Property(e => e.DatalinkColumnKey).HasColumnName("datalink_column_key");
                entity.Property(e => e.FunctionParameterKey).HasColumnName("function_parameter_key");
                entity.Property(e => e.ParameterName).IsRequired().HasColumnName("parameter_name").HasMaxLength(50);
                
                entity.Property(e => e.DataType).HasColumnName("datatype").HasMaxLength(20)
                    .HasConversion(
                        v => v.ToString(),
                        v => (DataType.ETypeCode) Enum.Parse(typeof(DataType.ETypeCode), v));
                entity.Property(e => e.IsGeneric).HasColumnName("is_generic");

                entity.Property(e => e.Direction).HasColumnName("direction").HasMaxLength(20)
                    .HasConversion(
                        v => v.ToString(),
                        v => (DexihParameterBase.EParameterDirection) Enum.Parse(typeof(DexihParameterBase.EParameterDirection), v));
                
                entity.Property(e => e.Position).HasColumnName("position");
                entity.Property(e => e.Rank).HasColumnName("rank");
                entity.Property(e => e.ListOfValues).HasColumnName("list_of_values").HasMaxLength(8000)
                    .HasConversion(
                        v => v == null ? null : string.Join("||", v),
                        v => string.IsNullOrEmpty(v) ? null : v.Split(new[] { "||" }, StringSplitOptions.None).ToArray());

                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");

                entity.HasOne(d => d.FunctionParameter)
                    .WithMany(p => p.ArrayParameters)
                    .HasForeignKey(d => d.FunctionParameterKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_function_array_parameter");
                

            });

            modelBuilder.Entity<DexihRemoteAgent>(entity =>
            {
                entity.HasKey(e => e.RemoteAgentKey).HasName("PK_dexih_remote_agent");

                entity.ToTable("dexih_remote_agents");

                entity.Property(e => e.RemoteAgentKey).HasColumnName("remote_agent_key");
                entity.Property(e => e.Name).IsRequired().HasColumnName("name").HasMaxLength(50);

                entity.Property(e => e.AllowExternalConnect).HasColumnName("allow_external_connect");
                entity.Property(e => e.IpAddresses).HasColumnName("ip_addresses").HasMaxLength(8000)
                    .HasConversion(
                        v => v == null ? null : string.Join(",", v),
                        v => string.IsNullOrEmpty(v) ? null : v.Split(new[] { "," }, StringSplitOptions.None).ToArray());

                entity.Property(e => e.UserId).HasColumnName("user_id").HasMaxLength(100);
                entity.Property(e => e.RemoteAgentId).IsRequired().HasColumnName("remote_agent_id").HasMaxLength(50);
                entity.Property(e => e.RestrictIp).HasColumnName("restrict_ip");
                
                entity.Property(e => e.HashedToken).HasColumnName("hashed_token").HasMaxLength(4000);
                
                entity.Property(e => e.LastLoginDateTime).HasColumnName("last_login_date");
                entity.Property(e => e.LastLoginIpAddress).HasColumnName("last_login_ip");

                entity.Property(e => e.CreateDate).HasColumnName("create_date");
				entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");
                
            });

            modelBuilder.Entity<DexihRemoteAgentHub>(entity =>
            {
                entity.HasKey(e => e.RemoteAgentHubKey).HasName("PK_dexih_remote_agent_hub");

                entity.ToTable("dexih_remote_agent_hubs");

                entity.Property(e => e.RemoteAgentHubKey).HasColumnName("remote_agent_hub_key");
                entity.Property(e => e.RemoteAgentKey).HasColumnName("remote_agent_key");
                entity.Property(e => e.IsDefault).HasColumnName("is_default");
                entity.Property(e => e.IsAuthorized).HasColumnName("is_authorized");
                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                
                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");
                
                entity.HasOne(d => d.Hub)
                    .WithMany(p => p.DexihRemoteAgentHubs)
                    .HasForeignKey(d => d.HubKey);
                
                entity.HasOne(d => d.RemoteAgent)
                    .WithMany(p => p.DexihRemoteAgentHubs)
                    .HasForeignKey(d => d.RemoteAgentKey);

            });

            modelBuilder.Entity<DexihHubUser>(entity =>
            {
                entity.HasKey(e => new { e.UserId, e.HubKey }).HasName("PK_UserHub");

                entity.ToTable("dexih_hub_user");

                entity.Property(e => e.UserId).HasMaxLength(450);
                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.Permission).HasColumnName("permission").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => (DexihHubUser.EPermission) Enum.Parse(typeof(DexihHubUser.EPermission), v));


                entity.Property(e => e.CreateDate).HasColumnName("create_date");
				entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");

                entity.HasOne(d => d.Hub)
                    .WithMany(p => p.DexihHubUsers)
                    .HasForeignKey(d => d.HubKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_user_hub_dexih_hubs");
            });

            modelBuilder.Entity<DexihHub>(entity =>
            {
                entity.HasKey(e => e.HubKey)
                    .HasName("PK_dexih_hubs");

                entity.ToTable("dexih_hubs");

                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.Name).HasColumnName("name").HasMaxLength(50);
                entity.Property(e => e.Description).HasColumnName("description").HasMaxLength(500);
                entity.Property(e => e.EncryptionKey).HasColumnName("encryption_key").HasMaxLength(255);
                entity.Property(e => e.SharedAccess).HasColumnName("shared_access").HasMaxLength(20)
                    .HasConversion(
                        v => v.ToString(),
                        v => (DexihHub.ESharedAccess) Enum.Parse(typeof(DexihHub.ESharedAccess), v));

                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");

            });

            modelBuilder.Entity<DexihHubVariable>(entity =>
            {
                entity.HasKey(e => e.HubVariableKey)
                    .HasName("PK_dexih_hub_variable");

                entity.ToTable("dexih_hub_variable");

                entity.Property(e => e.HubVariableKey).HasColumnName("hub_variable_key");
                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.Name).HasColumnName("name").HasMaxLength(50);
                entity.Property(e => e.Value).HasColumnName("value").HasMaxLength(1024);
                entity.Property(e => e.IsEncrypted).HasColumnName("is_encrypted");
                entity.Property(e => e.IsEnvironmentVariable).HasColumnName("is_environment_var");

                entity.Property(e => e.IsValid).HasColumnName("is_valid");
                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.UpdateDate).HasColumnName("update_date");

                entity.HasOne(d => d.Hub)
                      .WithMany(p => p.DexihHubVariables)
                    .HasForeignKey(d => d.HubKey);
            });

            modelBuilder.Entity<DexihTableColumn>(entity =>
            {
                entity.HasKey(e => e.ColumnKey)
                    .HasName("PK_dexih_table_columns");

                entity.ToTable("dexih_table_columns");

                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.ColumnKey).HasColumnName("column_key");

                entity.Property(e => e.AllowDbNull).HasColumnName("allow_db_null");

                entity.Property(e => e.Name).IsRequired().HasColumnName("name").HasMaxLength(250);
                entity.Property(e => e.ColumnValidationKey).HasColumnName("column_validation_key");
                entity.Property(e => e.DataType).HasColumnName("datatype").HasMaxLength(20)
                    .HasConversion(
                        v => v.ToString(),
                        v => (DataType.ETypeCode) Enum.Parse(typeof(DataType.ETypeCode), v));
                entity.Property(e => e.DeltaType).HasColumnName("delta_type").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => (TableColumn.EDeltaType) Enum.Parse(typeof(TableColumn.EDeltaType), v));
                entity.Property(e => e.Description).HasColumnName("description").HasMaxLength(1024);
                entity.Property(e => e.IsIncrementalUpdate).HasColumnName("is_incremental_update");
                entity.Property(e => e.IsMandatory).HasColumnName("is_mandatory");
                entity.Property(e => e.IsUnique).HasColumnName("is_unique");

                entity.Property(e => e.ParentColumnKey).HasColumnName("parent_column_key");

                entity.Property(e => e.LogicalName).HasColumnName("logical_name").HasMaxLength(250);
                entity.Property(e => e.ColumnGroup).HasColumnName("column_group").HasMaxLength(250);
                entity.Property(e => e.DefaultValue).HasColumnName("default_value").HasMaxLength(1024);
                entity.Property(e => e.IsUnicode).HasColumnName("is_unicode");
                entity.Property(e => e.MaxLength).HasColumnName("max_length");
                entity.Property(e => e.Position).HasColumnName("position");
                entity.Property(e => e.Precision).HasColumnName("precision");
                entity.Property(e => e.Scale).HasColumnName("scale");
                entity.Property(e => e.IsInput).HasColumnName("is_input");
                entity.Property(e => e.Rank).HasColumnName("rank");
                entity.Property(e => e.SecurityFlag).HasColumnName("security_flag").HasMaxLength(50)
                    .HasConversion(
                        v => v.ToString(),
                        v => (TableColumn.ESecurityFlag) Enum.Parse(typeof(TableColumn.ESecurityFlag), v));
                
                entity.Property(e => e.TableKey).HasColumnName("table_key");
                entity.Property(e => e.CreateDate).HasColumnName("create_date");
				entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");

                entity.HasOne(d => d.Table)
                    .WithMany(p => p.DexihTableColumns)
                    .HasForeignKey(d => d.TableKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_table_columns_dexih_tables");

                entity.HasOne(d => d.HubColumnValidation)
                    .WithMany(p => p.DexihColumnValidationColumn)
                    .HasForeignKey(d => d.ColumnValidationKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_table_columns_dexih_column_validations");
                
                entity.HasOne(d => d.ParentColumn)
                    .WithMany(p => p.ChildColumns)
                    .HasForeignKey(d => d.ParentColumnKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_table_child_columns");
                
            });

            modelBuilder.Entity<DexihTable>(entity =>
            {
                entity.HasKey(e => e.TableKey)
                    .HasName("PK_dexih_tables");

                entity.ToTable("dexih_tables");

                entity.Property(e => e.TableKey).HasColumnName("table_key");
				entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.ConnectionKey).HasColumnName("connection_key");

                entity.Property(e => e.Name).HasColumnName("name").HasMaxLength(250);
                entity.Property(e => e.Schema).HasColumnName("schema").HasMaxLength(250);
                entity.Property(e => e.Description).HasColumnName("description").HasMaxLength(1000);
                entity.Property(e => e.LogicalName).HasColumnName("logical_name").HasMaxLength(250);
                entity.Property(e => e.BaseTableName).HasColumnName("base_table_name").HasMaxLength(250);
                entity.Property(e => e.RejectedTableName).HasColumnName("rejected_table_name").HasMaxLength(250);

                entity.Property(e => e.UseQuery).HasColumnName("use_query");
                entity.Property(e => e.QueryString).HasColumnName("query_string");

                entity.Property(e => e.FileFormatKey).HasColumnName("file_format_key");
                entity.Property(e => e.AutoManageFiles).HasColumnName("auto_manage_files");
                entity.Property(e => e.UseCustomFilePaths).HasColumnName("use_custom_file_paths");
                entity.Property(e => e.FileMatchPattern).HasColumnName("file_match_pattern").HasMaxLength(255);
                entity.Property(e => e.FileRootPath).HasColumnName("file_root_path").HasMaxLength(1000);
                entity.Property(e => e.FileIncomingPath).HasColumnName("file_incoming_path").HasMaxLength(1000);
                entity.Property(e => e.FileOutgoingPath).HasColumnName("file_outgoing_path").HasMaxLength(1000);
                entity.Property(e => e.FileProcessedPath).HasColumnName("file_processed_path").HasMaxLength(1000);
                entity.Property(e => e.FileRejectedPath).HasColumnName("file_rejected_path").HasMaxLength(1000);
                entity.Property(e => e.SortColumnKeys).HasColumnName("sort_column_keys").HasMaxLength(500)
                    .HasConversion(
                        v => v == null ? null : string.Join(",", v),
                        v => string.IsNullOrEmpty(v) ? new long[0] : v.Split(",", StringSplitOptions.None).Select(long.Parse).ToArray());

                entity.Property(e => e.RestfulUri).HasColumnName("restful_uri").HasMaxLength(2000);
                entity.Property(e => e.RowPath).HasColumnName("row_path").HasMaxLength(2000);
                entity.Property(e => e.FormatType).HasColumnName("format_type").HasMaxLength(10)
                    .HasConversion(
                        v => v.ToString(),
                        v => (DataType.ETypeCode) Enum.Parse(typeof(DataType.ETypeCode), v));

                entity.Property(e => e.IsVersioned).HasColumnName("is_versioned");
                entity.Property(e => e.SourceConnectionName).HasColumnName("source_connection_name").HasMaxLength(250);
				entity.Property(e => e.IsShared).HasColumnName("is_shared");

				entity.Property(e => e.CreateDate).HasColumnName("create_date");
				entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");

                entity.HasOne(d => d.HubConnection)
                    .WithMany(p => p.DexihTables)
                    .HasForeignKey(d => d.ConnectionKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_tables_dexih_connections");

                entity.HasOne(d => d.HubFileFormat)
                    .WithMany(p => p.DexihTables)
                    .HasForeignKey(d => d.FileFormatKey)
                    .HasConstraintName("FK_dexih_tables_dexih_file_format");

            });

            modelBuilder.Entity<DexihTrigger>(entity =>
            {
                entity.HasKey(e => e.TriggerKey)
                    .HasName("PK_dexih_triggers");

                entity.ToTable("dexih_triggers");

                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.TriggerKey).HasColumnName("trigger_key");
                entity.Property(e => e.DatajobKey).HasColumnName("datajob_key");

                entity.Property(e => e.StartDate).HasColumnName("start_date");
                entity.Property(e => e.StartTime).HasColumnName("start_time");
                entity.Property(e => e.EndTime).HasColumnName("end_time");
                entity.Property(e => e.IntervalTime).HasColumnName("interval");
                entity.Property(e => e.DaysOfWeek).HasColumnName("days_of_week").HasMaxLength(200)
                    .HasConversion(
                        v => String.Join(",", v.Select(c=>c.ToString())),
                        v => string.IsNullOrEmpty(v) ? new DexihTrigger.EDayOfWeek[] { } : v.Split(',', StringSplitOptions.None).Select(c => (DexihTrigger.EDayOfWeek)Enum.Parse(typeof(DexihTrigger.EDayOfWeek), c)).ToArray());
                entity.Property(e => e.MaxRecurs).HasColumnName("max_recurrs");
                entity.Property(e => e.CronExpression).HasColumnName("cron_expression").HasMaxLength(500);

                entity.Property(e => e.CreateDate).HasColumnName("create_date");
				entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");

                entity.HasOne(d => d.Datajob).WithMany(p => p.DexihTriggers)
                    .HasForeignKey(d => d.DatajobKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_triggers_dexih_datajobs");
            });
            
            modelBuilder.Entity<DexihView>(entity =>
            {
                entity.HasKey(e => e.ViewKey).HasName("PK_dexih_view");

                entity.ToTable("dexih_views");

                entity.Property(e => e.ViewKey).HasColumnName("view_key");
                entity.Property(e => e.HubKey).IsRequired().HasColumnName("hub_key");
                entity.Property(e => e.Name).IsRequired().HasColumnName("name").HasMaxLength(250);
                entity.Property(e => e.Description).HasColumnName("description");
                entity.Property(e => e.ViewType).IsRequired().HasColumnName("view_type").HasMaxLength(20)
                    .HasConversion(
                        v => v.ToString(),
                        v => (DexihView.EViewType) Enum.Parse(typeof(DexihView.EViewType), v));
                entity.Property(e => e.SourceType).IsRequired().HasColumnName("source_type").HasMaxLength(20)
                    .HasConversion(
                        v => v.ToString(),
                        v => (ESourceType) Enum.Parse(typeof(ESourceType), v));
                entity.Property(e => e.SourceDatalinkKey).HasColumnName("source_datalink_key");
                entity.Property(e => e.SourceTableKey).HasColumnName("source_table_key");
                entity.Property(e => e.Name).HasColumnName("name").HasMaxLength(250);

                entity.Property(e => e.SelectQuery).HasColumnName("select_query")
                    .HasConversion(
                        v => v == null ? null : JsonConvert.SerializeObject(v),
                        v => v == null ? null : JsonConvert.DeserializeObject<SelectQuery>(v));
                
                entity.Property(e => e.ChartConfig).HasColumnName("chart_config")
                    .HasConversion(
                        v => v == null ? null : JsonConvert.SerializeObject(v),
                        v => v == null ? null : JsonConvert.DeserializeObject<ChartConfig>(v));
                
                entity.Property(e => e.InputValues).HasColumnName("input_values")
                    .HasConversion(
                        v => v == null ? null : JsonConvert.SerializeObject(v),
                        v => v == null ? null : JsonConvert.DeserializeObject<InputColumn[]>(v));

                entity.Property(e => e.CreateDate).HasColumnName("create_date");
                entity.Property(e => e.UpdateDate).HasColumnName("update_date");
                entity.Property(e => e.IsValid).HasColumnName("is_valid");

            });

        }

        public DbSet<DexihColumnValidation> DexihColumnValidation { get; set; }
        public DbSet<DexihConnection> DexihConnections { get; set; }
	    public DbSet<DexihCustomFunction> DexihCustomFunctions { get; set; }
        public DbSet<DexihDatajob> DexihDatajobs { get; set; }
        public DbSet<DexihDatalinkDependency> DexihDatalinkDependencies { get; set; }
        public DbSet<DexihDatalinkTable> DexihDatalinkTables { get; set; }
        public DbSet<DexihDatalinkColumn> DexihDatalinkColumns { get; set; }
        public DbSet<DexihDatalinkProfile> DexihDatalinkProfiles { get; set; }
        public DbSet<DexihDatalinkStep> DexihDatalinkStep { get; set; }
	    public DbSet<DexihDatalinkStepColumn> DexihDatalinkStepColumns { get; set; }
        public DbSet<DexihDatalinkTarget> DexihDatalinkTargets { get; set; }
	    public DbSet<DexihDatalinkTest> DexihDatalinkTests { get; set; }
	    public DbSet<DexihDatalinkTestStep> DexihDatalinkTestSteps { get; set; }
	    public DbSet<DexihDatalinkTestTable> DexihDatalinkTestTables { get; set; }
        public DbSet<DexihDatalinkTransformItem> DexihDatalinkTransformItems { get; set; }
        public DbSet<DexihDatalinkTransform> DexihDatalinkTransforms { get; set; }
        public DbSet<DexihDatalink> DexihDatalinks { get; set; }
        public DbSet<DexihFileFormat> DexihFileFormat { get; set; }
        public DbSet<DexihFunctionParameter> DexihFunctionParameters { get; set; }
	    public DbSet<DexihFunctionArrayParameter> DexihFunctionArrayParameters { get; set; }
        public DbSet<DexihRemoteAgent> DexihRemoteAgents { get; set; }
	    public DbSet<DexihRemoteAgentHub> DexihRemoteAgentHubs { get; set; }
        public DbSet<DexihHubUser> DexihHubUser { get; set; }
        public DbSet<DexihHub> DexihHubs { get; set; }
        public DbSet<DexihHubVariable> DexihHubVariable { get; set; }
        public DbSet<DexihTableColumn> DexihTableColumns { get; set; }
        public DbSet<DexihTable> DexihTables { get; set; }
        public DbSet<DexihTrigger> DexihTriggers { get; set; }
	    public DbSet<DexihView> DexihViews { get; set; }
    }
}
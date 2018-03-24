using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions.Query;
using Microsoft.EntityFrameworkCore;
using Microsoft.AspNetCore.Identity;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    public class SeedData
    {
        private static readonly DateTime CurrentDate = DateTime.Now;

        public async Task UpdateReferenceData(DexihRepositoryContext repoDbContext, RoleManager<IdentityRole> roleManager, UserManager<ApplicationUser> userManager)
        {
            try
            {
                if (roleManager != null && userManager != null)
                {
                    var roles = new string[] { "ADMINISTRATOR", "MANAGER", "USER", "VIEWER" };

                    foreach (var role in roles)
                    {
                        if (!(await roleManager.RoleExistsAsync(role)))
                        {
                            var identityRole = new IdentityRole(role);
                            var identityResult = await roleManager.CreateAsync(identityRole);

                            if (!identityResult.Succeeded)
                            {
                                //configurationLogger.LogCritical(
                                //    String.Format("!identityResult.Succeeded after roleManager.CreateAsync(identityRole) for identityRole with roleName { 0} ", role));
                                //foreach (var error in identityResult.Errors)
                                //{
                                //    configurationLogger.LogCritical(
                                //        String.Format(
                                //            "identityResult.Error.Description: {0}",
                                //            error.Description));
                                //    configurationLogger.LogCritical(
                                //        String.Format(
                                //            "identityResult.Error.Code: {0}",
                                //         error.Code));
                                //}
                            }
                        }
                    }

                    var user = new ApplicationUser
                    {
                        UserName = "admin@dataexpertsgroup.com",
                        Email = "admin@dataexpertsgroup.com",
                        EmailConfirmed = true,
						IsInvited = true,
						IsEnabled = true,
						IsRegistered = true,
						Terms = true,
						Subscription = false,
						FirstName = "Admin",
						LastName = "User",
                        HubQuota = 9999,
                        InviteQuota = 9999
                    };

                    var adminUser = await userManager.FindByNameAsync("admin@dataexpertsgroup.com");
                    if (adminUser == null)
                    {
                        var result = await userManager.CreateAsync(user, "dexIH-1");
                        //configurationLogger.LogDebug("admin@dataexpertsgroup.com user created.");
                        adminUser = await userManager.FindByNameAsync("admin@dataexpertsgroup.com");
                    }

                    if (!(await userManager.IsInRoleAsync(adminUser, "ADMINISTRATOR")))
                    {
                        await userManager.AddToRoleAsync(adminUser, "ADMINISTRATOR");
                        //configurationLogger.LogDebug("admin@dataexpertsgroup.com  added to administrator role");
                    }
                }

                var internalHub = await repoDbContext.DexihHubs.FirstOrDefaultAsync(c => c.IsInternal);
                if (internalHub == null)
                {
                    internalHub = new DexihHub()
                    {
                        HubKey = 0,
                        IsInternal = true,
                        IsValid = true,
                        Name = "Internal Hub - not for use."
                    };

                    await AddOrUpdateAsync(repoDbContext, b => b.HubKey, new DexihHub[] { internalHub });
                }

				var internalHubKey = repoDbContext.DexihHubs.First(c => c.IsInternal && c.IsValid).HubKey;

				//add reference data
                var settings = GetSettings();
                await AddOrUpdateAsync(repoDbContext, b => b.Name, settings);

                if (!repoDbContext.DexihFileFormat.Any())
                {
                    var fileFormats = GetFileFormats(internalHubKey);
                    await AddOrUpdateAsync(repoDbContext, b => b.FileFormatKey, fileFormats);
                }

            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private async Task AddOrUpdateAsync<TEntity>(
            DexihRepositoryContext repoDbContext,
            Func<TEntity, object> propertyToMatch, IEnumerable<TEntity> entities)
            where TEntity : class
        {
            var entries = await repoDbContext.Set<TEntity>().ToListAsync();

            foreach (var item in entities)
            {
                //var entry = await repoDbContext.Set<TEntity>().SingleOrDefaultAsync(g => propertyToMatch(g).Equals(propertyToMatch(item)));
                var entry = entries.SingleOrDefault(g => propertyToMatch(g).Equals(propertyToMatch(item)));
                if(entry == null)
                {
                    repoDbContext.Set<TEntity>().Add(item);
                } else
                {
                    item.CopyProperties(entry, true);
                }
            }

            await repoDbContext.SaveChangesAsync();
        }

        public DexihFileFormat[] GetFileFormats(long internalHubKey)
        {
            var fileFormats = new DexihFileFormat[]
            {
                new DexihFileFormat {HubKey = internalHubKey, Name = "Comma delimited, headers", IsDefault = true, AllowComments = false, BufferSize = 2048, Comment = '#', Delimiter=",", DetectColumnCountChanges = false, HasHeaderRecord = true, IgnoreHeaderWhiteSpace = false, IgnoreReadingExceptions = false, IgnoreQuotes = false, Quote = '\"', QuoteAllFields = false, QuoteNoFields = false, SkipEmptyRecords = false, TrimFields = false, TrimHeaders = false, WillThrowOnMissingField = true, CreateDate = CurrentDate, UpdateDate = CurrentDate, IsValid = true },
                new DexihFileFormat {HubKey = internalHubKey, Name = "Comma delimited, no headers", IsDefault = true, AllowComments = false, BufferSize = 2048, Comment = '#', Delimiter=",", DetectColumnCountChanges = false, HasHeaderRecord = false, IgnoreHeaderWhiteSpace = false, IgnoreReadingExceptions = false, IgnoreQuotes = false, Quote = '\"', QuoteAllFields = false, QuoteNoFields = false, SkipEmptyRecords = false, TrimFields = false, TrimHeaders = false, WillThrowOnMissingField = true, CreateDate = CurrentDate, UpdateDate = CurrentDate, IsValid = true },
                new DexihFileFormat {HubKey = internalHubKey, Name = "No delimiter, no headers", IsDefault = true, AllowComments = false, BufferSize = 2048, Comment = '#', Delimiter="None!@#$", DetectColumnCountChanges = false, HasHeaderRecord = false, IgnoreHeaderWhiteSpace = false, IgnoreReadingExceptions = false, IgnoreQuotes = false, Quote = '\"', QuoteAllFields = false, QuoteNoFields = false, SkipEmptyRecords = false, TrimFields = false, TrimHeaders = false, WillThrowOnMissingField = true, CreateDate = CurrentDate, UpdateDate = CurrentDate, IsValid = true },
            };

            return fileFormats;
        }


        public DexihSetting[] GetSettings()
        {
            var settings = new DexihSetting[]
            {
                new DexihSetting() {Category = "Naming", Name =  "General.Table.Name", Value =  "{0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "Stage.Table.Name", Value =  "stg{0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "Validate.Table.Name", Value =  "val{0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "Transform.Table.Name", Value =  "trn{0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "Deliver.Table.Name", Value =  "{0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
				new DexihSetting() {Category = "Naming", Name =  "Publish.Table.Name", Value =  "{0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "Share.Table.Name", Value =  "{0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "General.Table.Description", Value =  "Data from the table {0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
				new DexihSetting() {Category = "Naming", Name =  "Stage.Table.Description", Value =  "The staging table for {0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "Validate.Table.Description", Value =  "The validation table for {0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "Transform.Table.Description", Value =  "The transform table for {0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "Deliver.Table.Description", Value =  "The delivered table for {0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
				new DexihSetting() {Category = "Naming", Name =  "Publish.Table.Description", Value =  "The published data for {0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "Share.Table.Description", Value =  "Data from the table {0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "Table.RejectName", Value =  "Reject{0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "Table.ProfileName", Value =  "Profile{0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "General.Datalink.Name", Value =  "Data load for {0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "Stage.Datalink.Name", Value =  "Staging load for {0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "Validate.Datalink.Name", Value =  "Validation load for {0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "Transform.Datalink.Name", Value =  "Transform load for {0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "Deliver.Datalink.Name", Value =  "Deliver load for {0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "Publish.Datalink.Name", Value =  "Publish load for {0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "Share.Datalink.Name", Value =  "Data for {0}", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "CreateDate.Column.Name", Value =  "CreateDate", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "CreateDate.Column.Logical", Value =  "CreateDate", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "CreateDate.Column.Description", Value =  "The date and time the record first created.", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "UpdateDate.Column.Name", Value =  "UpdateDate", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "UpdateDate.Column.Logical", Value =  "UpdateDate", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "UpdateDate.Column.Description", Value =  "The date and time the record last updated.", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "CreateAuditKey.Column.Name", Value =  "CreateAuditKey", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "CreateAuditKey.Column.Logical", Value =  "CreateAuditKey", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "CreateAuditKey.Column.Description", Value =  "Link to the audit key that created the record.", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "UpdateAuditKey.Column.Name", Value =  "UpdateAuditKey", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "UpdateAuditKey.Column.Logical", Value =  "UpdateAuditKey", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "UpdateAuditKey.Column.Description", Value =  "Link to the audit key that updated the record.", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "SurrogateKey.Column.Name", Value =  "{0}Sk", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "SurrogateKey.Column.Logical", Value =  "{0}Sk", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "SurrogateKey.Column.Description", Value =  "The surrogate key created for the table {0}.", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "ValidFromDate.Column.Name", Value =  "ValidFromDate", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "ValidFromDate.Column.Logical", Value =  "ValidFromDate", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "ValidFromDate.Column.Description", Value =  "The date and time the record becomes valid.", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "ValidToDate.Column.Name", Value =  "ValidToDate", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "ValidToDate.Column.Logical", Value =  "ValidToDate", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "ValidToDate.Column.Description", Value =  "The date and time the record becomes invalid.", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "IsCurrentField.Column.Name", Value =  "IsCurrent", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "IsCurrentField.Column.Logical", Value =  "IsCurrent", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
				new DexihSetting() {Category = "Naming", Name =  "IsCurrentField.Column.Description", Value =  "True/False - Is the current record within the valid range?", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "SourceSurrogateKey.Column.Name", Value =  "SourceSk", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "SourceSurrogateKey.Column.Logical", Value =  "SourceSk", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "SourceSurrogateKey.Column.Description", Value =  "The surrogate key from the source table.", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "ValidationStatus.Column.Name", Value =  "ValidationStatus", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "ValidationStatus.Column.Logical", Value =  "ValidationStatus", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true },
                new DexihSetting() {Category = "Naming", Name =  "ValidationStatus.Column.Description", Value =  "Indicates if the record has passed validation tests.", CreateDate = DateTime.Now, UpdateDate = DateTime.Now, IsValid = true }
            };

            return settings;
        }

    }
}

using dexih.functions;
using dexih.functions.Query;
using dexih.repository;
using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions.BuiltIn;
using static dexih.functions.FunctionReference;
using static dexih.repository.DexihColumnValidation;
using static Dexih.Utils.DataType.DataType;

namespace dexih.operations
{
    public class ColumnValidationRun
    {
        public ColumnValidationRun(TransformSettings transformSettings, DexihColumnValidation columnValidation, DexihHub hub)
        {
            ColumnValidation = columnValidation;
            Hub = hub;
            _transformSettings = transformSettings;

        }

        /// <summary>
        /// This should be set to the target column default value when the function is initialized.
        /// </summary>
        public object DefaultValue { get; set; }

        private DexihColumnValidation ColumnValidation { get; }
        private DexihHub Hub { get; }

        private readonly TransformSettings _transformSettings;

        private ConditionFunctions _conditionFunctions;
        private Transform _lookup;
        private TableColumn _lookupColumn;

        private int ValidationPassCount { get; set; }
        private int ValidationFailCount { get; set; }

        public TransformFunction GetValidationFunction(string columnName)
        {
            var validationFunction = new TransformFunction(
                this, 
                this.GetType().GetMethod("Run"), 
                new TableColumn[] { new TableColumn(columnName) }, 
                new TableColumn(columnName), new TableColumn[] { new TableColumn(columnName), new TableColumn("RejectReason") }
                )
            {
                InvalidAction = ColumnValidation.InvalidAction
            };
            return validationFunction;
        }

        public bool Run(object value, out object cleanedValue, out string rejectReason)
        {
            var validateResult = ValidateClean(value, CancellationToken.None).Result;
            cleanedValue = validateResult.cleanedValue;
            rejectReason = validateResult.rejectReason;
            return validateResult.success;
        }

        /// <summary>
        /// Run method is used by the "Function" class to execute a validation call.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<(bool, object cleanedValue, string rejectReason)> RunAsync(object value, CancellationToken cancellationToken)
        {
            var validateResult = await ValidateClean(value, cancellationToken);
            return (validateResult.success, validateResult.cleanedValue, validateResult.rejectReason);
        }

        public async Task<(bool success, object cleanedValue, string rejectReason)> ValidateClean(object value, CancellationToken cancellationToken)
        {
            var validate = await Validate(value, cancellationToken);
            //if validation passes, return true
            if (validate.success)
            {
                ValidationPassCount++;
                return (true, null, "");
            }
            else
            {
                ValidationFailCount++;
                object cleanedValue;

                if (ColumnValidation.InvalidAction == TransformFunction.EInvalidAction.Clean || ColumnValidation.InvalidAction == TransformFunction.EInvalidAction.RejectClean)
                {

                    switch (ColumnValidation.CleanAction)
                    {
                        case ECleanAction.Blank:
                            cleanedValue = "";
                            break;
                        case ECleanAction.Null:
                            cleanedValue = DBNull.Value;
                            break;
                        case ECleanAction.DefaultValue:
                            cleanedValue = DefaultValue;
                            break;
                        case ECleanAction.CleanValue:
                            cleanedValue = ColumnValidation.CleanValue;
                            break;
                        case ECleanAction.Truncate:
                            if (ColumnValidation.MaxLength == null)
                                return (false, null, "The clean rule for " + ColumnValidation.Name + " failed to truncate as a MaxLength value has not been set.");
                            cleanedValue = value.ToString().Substring(0, (int)ColumnValidation.MaxLength);
                            break;
                        case ECleanAction.OriginalValue:
                            cleanedValue = value;
                            break;
                        default:
                            cleanedValue = null;
                            break;
                    }
                }
                else
                    cleanedValue = null;

                return (false, cleanedValue, validate.reason);
            }

        }

        public async Task<(bool success, string reason)> Validate(object value, CancellationToken cancellationToken)
        {
            try
            {
                //test for null
                if (value == null || value is DBNull)
                {
                    if (ColumnValidation.AllowDbNull == false)
                    {
                        return (false, "Tried to insert null into non-null column.");
                    }
                    else //if the value is null, return valid and don't test any other rules.
                    {
                        return (true, "");
                    }
                }

                var stringValue = value.ToString();
                var basicDataType = GetBasicType(ColumnValidation.DataType);

                //test for datatype
                var result = TryParse(ColumnValidation.DataType, value);

                if (result == null)
                {
                    return (false, $"The value return a null when attempting to convert to datatype {ColumnValidation.DataType}.");
                }

                if (ColumnValidation.MaxLength != null && stringValue.Length > ColumnValidation.MaxLength)
                {
                    return (false, "The value has a string length of " + stringValue.Length + " which exceeds the maximum length of " + ColumnValidation.MaxLength);
                }
                if (ColumnValidation.MinLength != null && stringValue.Length < ColumnValidation.MinLength)
                {
                    return (false, "The value has a string length of " + stringValue.Length + " which is below the minimum length of " + ColumnValidation.MinLength);
                }

                if (basicDataType == EBasicType.Date || basicDataType == EBasicType.Numeric)
                {
                    object compareValue;
                    //convert to a double to enable compare routines to work
                    try
                    {
                        compareValue = TryParse(ETypeCode.Decimal, value);
                    }
                    catch (Exception ex)
                    {
                        return (false, $"The value failed to convert to a Decimal.  {ex.Message}.");
                    }

                    if (ColumnValidation.MaxValue != null && (decimal)compareValue > ColumnValidation.MaxValue)
                    {
                        return (false, "The value has a numerical value of " + stringValue + " which exceeds the maximum value of " + ColumnValidation.MaxValue);
                    }
                    if (ColumnValidation.MinValue != null && (decimal)compareValue < ColumnValidation.MinValue)
                    {
                        return (false, "The value has a numerical value of " + stringValue + " which is below the minimum Value of " + ColumnValidation.MinValue);
                    }
                }

                if (ColumnValidation.PatternMatch != null)
                {
                    if (_conditionFunctions == null) _conditionFunctions = new ConditionFunctions();
                    if (_conditionFunctions.IsPattern(stringValue, ColumnValidation.PatternMatch) == false)
                    {
                        return (false, "The value \"" + stringValue + "\" does not match the pattern " + ColumnValidation.PatternMatch);
                    }
                }

                if (ColumnValidation.RegexMatch != null)
                {
                    if (_conditionFunctions == null) _conditionFunctions = new ConditionFunctions();
                    if (_conditionFunctions.RegexMatch(stringValue, ColumnValidation.RegexMatch) == false)
                    {
                        return (false, "The value \"" + stringValue + "\" does not match the regular expression " + ColumnValidation.RegexMatch);
                    }
                }

                if (ColumnValidation.ListOfValues != null && ColumnValidation.ListOfValues.Length > 0 && ColumnValidation.ListOfValues?.Contains(stringValue) == false)
                {
                    return (false, "The value \"" + stringValue + "\" was not found in the restricted list of values.");
                }

                if (ColumnValidation.ListOfNotValues != null && ColumnValidation.ListOfNotValues.Length > 0 && ColumnValidation.ListOfNotValues?.Contains(stringValue) == true)
                {
                    return (false, "The value \"" + stringValue + "\" was found in the excluded list of values.");
                }


                if (ColumnValidation.LookupColumnKey != null)
                {
                    DexihTableColumn dbColumn = null;
                    DexihTable dbTable = null;

                    if (_lookup == null)
                    {
                        dbColumn = Hub.GetColumnFromKey((long)ColumnValidation.LookupColumnKey);
                        if (dbColumn == null)
                        {
                            return (false, "Error: The lookup table could not be found.");
                        }

                        dbTable = Hub.GetTableFromKey(dbColumn.TableKey);
                        if (dbTable == null)
                        {
                            return (false, "Error: The lookup table could not be found.");
                        }

                        var dbConnection = Hub.DexihConnections.SingleOrDefault(c => c.ConnectionKey == dbTable.ConnectionKey);
                        var connection = dbConnection.GetConnection(_transformSettings);
                        var table = dbTable.GetTable(connection, _transformSettings);
                        _lookupColumn = dbColumn.GetTableColumn();

                        _lookup = connection.GetTransformReader(table);
                        await _lookup.Open(0, null, cancellationToken);
                        _lookup.SetCacheMethod(Transform.ECacheMethod.OnDemandCache);
                    }

                    var filter = new Filter(_lookupColumn.Name, Filter.ECompare.IsEqual, stringValue);
                    var lookupReturn = await _lookup.LookupRow(new List<Filter>() { filter }, Transform.EDuplicateStrategy.First, cancellationToken);
                    if (lookupReturn == null || !lookupReturn.Any())
                    {
                        return (false, $"The validation lookup on table {dbTable?.Name} column {dbColumn?.Name}");
                    }
                }

                return (true, null);
            }
            catch (Exception ex)
            {
                throw new ColumnValidationException($"The column validation {ColumnValidation.Name} failed.  {ex.Message}", ex);
            }
        }
    }
}

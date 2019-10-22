using dexih.functions;
using dexih.repository;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions.Parameter;
using dexih.transforms.Mapping;
using Dexih.Utils.DataType;
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

        private HashSet<object> _lookupValues;
        private Table _lookupTable;
        private TableColumn _lookupColumn;

        private int ValidationPassCount { get; set; }
        private int ValidationFailCount { get; set; }

        public Mapping GetValidationMapping(string columnName)
        {    
            var parameters = new Parameters()
            {
                Inputs = new Parameter[] {new ParameterColumn(columnName, ETypeCode.String)},
                ReturnParameters = new Parameter[] { new ParameterOutputColumn(columnName, ETypeCode.String)},
                Outputs = new Parameter[] {new ParameterOutputColumn(columnName, ETypeCode.String), new ParameterOutputColumn("RejectReason", new TableColumn("RejectReason", EDeltaType.RejectedReason)) }
            };
            
            var validationFunction = new TransformFunction(this, GetType().GetMethod(nameof(Run)), null, parameters, null);
            validationFunction.InitializeMethod = new TransformMethod(GetType().GetMethod(nameof(Initialize)));
            var mapping = new MapValidation(validationFunction, parameters);
            return mapping;
        }

        public async Task Initialize(CancellationToken cancellationToken)
        {
            if (ColumnValidation.LookupColumnKey != null)
            {
                var tableColumn = Hub.GetTableColumnFromKey((long)ColumnValidation.LookupColumnKey);
                if (tableColumn.column == null)
                {
                    throw new ColumnValidationException($"Error: The lookup table for column {ColumnValidation.LookupColumnKey} could not be found.");
                }

                var dbConnection = Hub.DexihConnections.Single(c => c.Key == tableColumn.table.ConnectionKey);
                var connection = dbConnection.GetConnection(_transformSettings);
                _lookupTable = tableColumn.table.GetTable(Hub, connection, _transformSettings);
                _lookupColumn = tableColumn.column.GetTableColumn(null);
                _lookupValues = await connection.GetColumnValues(_lookupTable, _lookupColumn, cancellationToken);
            }
        }

        public bool Run(object value, out object cleanedValue, out string rejectReason)
        {
            var validateResult = ValidateClean(value);
            cleanedValue = validateResult.cleanedValue;
            rejectReason = validateResult.rejectReason;
            return validateResult.success;
        }

        public (bool success, object cleanedValue, string rejectReason) ValidateClean(object value)
        {
            var validate = Validate(value);
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
                            cleanedValue = value?.ToString().Substring(0, (int)ColumnValidation.MaxLength);
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

        public (bool success, string reason) Validate(object value)
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

                //test for datatype
                var parsedValue = Operations.Parse(ColumnValidation.DataType, value);

                if (parsedValue == null)
                {
                    return (false, $"The value return a null when attempting to convert to datatype {ColumnValidation.DataType}.");
                }

                if (parsedValue is string stringValue)
                {
                    if (ColumnValidation.MaxLength != null && stringValue.Length > ColumnValidation.MaxLength)
                    {
                        return (false,
                            "The value has a string length of " + stringValue.Length +
                            " which exceeds the maximum length of " + ColumnValidation.MaxLength);
                    }

                    if (ColumnValidation.MinLength != null && stringValue.Length < ColumnValidation.MinLength)
                    {
                        return (false,
                            "The value has a string length of " + stringValue.Length +
                            " which is below the minimum length of " + ColumnValidation.MinLength);
                    }
                    
                    if (!string.IsNullOrEmpty(ColumnValidation.PatternMatch))
                    {
                        if (stringValue.IsPattern(ColumnValidation.PatternMatch) == false)
                        {
                            return (false, "The value \"" + stringValue + "\" does not match the pattern " + ColumnValidation.PatternMatch);
                        }
                    }

                    if (!string.IsNullOrEmpty(ColumnValidation.RegexMatch))
                    {
                        if (Regex.Match(stringValue, ColumnValidation.RegexMatch).Success == false)
                        {
                            return (false, "The value \"" + stringValue + "\" does not match the regular expression " + ColumnValidation.RegexMatch);
                        }
                    }
                }

                if (ColumnValidation.MaxValue != null && Operations.Compare(ColumnValidation.DataType, value, ColumnValidation.MaxValue) == 1)
                {
                    return (false, "The value is " + parsedValue + " which exceeds the maximum value of " + ColumnValidation.MaxValue);
                }
                if (ColumnValidation.MinValue != null  && Operations.Compare(ColumnValidation.DataType, value, ColumnValidation.MinValue) == -1)
                {
                    return (false, "The value is " + parsedValue + " which is below the minimum Value of " + ColumnValidation.MinValue);
                }

                if (ColumnValidation.ListOfValues != null && ColumnValidation.ListOfValues.Length > 0 && ColumnValidation.ListOfValues?.Contains(value) == false)
                {
                    return (false, "The value \"" + value + "\" was not found in the restricted list of values.");
                }

                if (ColumnValidation.ListOfNotValues != null && ColumnValidation.ListOfNotValues.Length > 0 && ColumnValidation.ListOfNotValues?.Contains(value) == true)
                {
                    return (false, "The value \"" + value + "\" was found in the excluded list of values.");
                }


                if (ColumnValidation.LookupColumnKey != null && _lookupValues != null)
                {
                    var convertedValue = Operations.Parse(_lookupColumn.DataType, _lookupColumn.Rank, value);
                    if (!_lookupValues.Contains(convertedValue))
                    {
                        return (false, $"The value {value} could not be found on the lookup table {_lookupTable?.Name} column {_lookupColumn?.Name}");
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

using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations.Schema;
using Newtonsoft.Json.Converters;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using static Dexih.Utils.DataType.DataType;
using dexih.functions.Query;
using System.Linq;
using System.Text;
using System.IO;
using System.Reflection;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using System.Collections;
using System.Diagnostics;
using System.Security.Authentication;
using System.Threading;
using dexih.functions.Mappings;
using dexih.functions.Parameter;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Serialization;
using static dexih.functions.Query.SelectColumn;
using Dexih.Utils.DataType;

namespace dexih.repository
{
	public class DexihDatalinkTransformItem : DexihBaseEntity
	{
		[JsonConverter(typeof(StringEnumConverter))]
		public enum ETransformItemType
		{
			BuiltInFunction, CustomFunction, ColumnPair, JoinPair, Sort, Column, FilterPair, AggregatePair, Series
		}

		public DexihDatalinkTransformItem() => DexihFunctionParameters = new HashSet<DexihFunctionParameter>();

		[JsonIgnore, CopyIgnore]
		public long HubKey { get; set; }

		[CopyCollectionKey((long)0, true)]
		public long DatalinkTransformItemKey { get; set; }

		[CopyParentCollectionKey]
		public long DatalinkTransformKey { get; set; }

		public int Position { get; set; }

		public ETransformItemType TransformItemType { get; set; }

		public long? TargetDatalinkColumnKey { get; set; }
		public long? SourceDatalinkColumnKey { get; set; }
		public long? JoinDatalinkColumnKey { get; set; }
		public long? FilterDatalinkColumnKey { get; set; }

		public string SourceValue { get; set; }
		public string JoinValue { get; set; }
		public string FilterValue { get; set; }

		public string FunctionClassName { get; set; }
		public string FunctionAssemblyName { get; set; }
		public string FunctionMethodName { get; set; }
        public bool IsGeneric { get; set; }
        public ETypeCode? GenericTypeCode { get; set; }

		public long? CustomFunctionKey { get; set; }
		
		public Sort.EDirection? SortDirection { get; set; }

		public Filter.ECompare? FilterCompare { get; set; }

        public EAggregate? Aggregate { get; set; }

		public ESeriesGrain? SeriesGrain { get; set; }
		public bool SeriesFill { get; set; }
		public string SeriesStart { get; set; }
		public string SeriesFinish { get; set; }

		public string FunctionCode { get; set; }
		public string FunctionResultCode { get; set; }

		public EErrorAction OnError { get; set; }
		public EErrorAction OnNull { get; set; }
		public bool NotCondition { get; set; }

		public TransformFunction.EInvalidAction InvalidAction { get; set; }

		[NotMapped, CopyIgnore]
		public EntityStatus EntityStatus { get; set; }

		public ICollection<DexihFunctionParameter> DexihFunctionParameters { get; set; }

		[JsonIgnore, CopyIgnore]
		public virtual DexihDatalinkTransform Dt { get; set; }

		[CopyIgnore]
		public virtual DexihDatalinkColumn SourceDatalinkColumn { get; set; }
		[CopyIgnore]
		public virtual DexihDatalinkColumn TargetDatalinkColumn { get; set; }
		[CopyIgnore]
		public virtual DexihDatalinkColumn JoinDatalinkColumn { get; set; }
		[CopyIgnore]
		public virtual DexihDatalinkColumn FilterDatalinkColumn { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihCustomFunction CustomFunction { get; set; }

		private Parameter ConvertParameter(DexihFunctionParameterBase parameter, DexihParameterBase.EParameterDirection direction)
		{

			var column = parameter.DatalinkColumn?.GetTableColumn(null);
			
			if (direction == DexihParameterBase.EParameterDirection.Input || direction == DexihParameterBase.EParameterDirection.ResultInput)
			{
				if (column != null)
				{
					return new ParameterColumn(parameter.ParameterName, parameter.DataType, parameter.Rank, column);
				}
				else
				{
					return new ParameterValue(parameter.ParameterName, parameter.DataType, parameter.Value);
				}
			}
			else
			{
				return new ParameterOutputColumn(parameter.ParameterName, parameter.DataType, parameter.Rank, column);				
			}
		}
		
        /// <summary>
        /// Creates a reference to a compiled version of the mapping function.
        /// </summary>
        /// <returns></returns>
        public (TransformFunction function, Parameters parameters) CreateFunctionMethod(DexihHub hub, GlobalVariables globalVariables, bool createConsoleSample = false, ILogger logger = null)
		{
			try
			{
				var timer = Stopwatch.StartNew();
				logger?.LogTrace($"GetFunctionMethod, started.");

				if (TransformItemType != ETransformItemType.CustomFunction &&
				    TransformItemType != ETransformItemType.BuiltInFunction)
				{
					throw new RepositoryException("The datalink transform item is not a custom function");
				}

				//create the input & output parameters
				var inputs = new List<Parameter>();
				var outputs = new List<Parameter>();
				var resultInputs = new List<Parameter>();
				var resultOutputs = new List<Parameter>();
				Parameter returnParameter = null;
				Parameter resultReturnParameter = null;
				
				

				foreach (var parameter in DexihFunctionParameters.OrderBy(c=>c.Position))
				{
					Parameter newParameter;
					if (parameter.Rank > 0)
					{
						if (parameter.ArrayParameters.Count > 0)
						{
							var arrayParameters = new List<Parameter>();

							foreach (var arrayParameter in parameter.ArrayParameters)
							{
								arrayParameters.Add(ConvertParameter(arrayParameter, arrayParameter.Direction));
							}

							newParameter = new ParameterArray(parameter.ParameterName, parameter.DataType, parameter.Rank, arrayParameters);
						}
						else
						{
							newParameter = ConvertParameter(parameter, parameter.Direction);
						}
					}
					else
					{
						newParameter = ConvertParameter(parameter, parameter.Direction);
					}

					switch (parameter.Direction)
					{
						case DexihParameterBase.EParameterDirection.Input:
							inputs.Add(newParameter);
							break;
						case DexihParameterBase.EParameterDirection.Output:
							outputs.Add(newParameter);
							break;
						case DexihParameterBase.EParameterDirection.ResultInput:
							resultInputs.Add(newParameter);
							break;
						case DexihParameterBase.EParameterDirection.ResultOutput:
							resultOutputs.Add(newParameter);
							break;
						case DexihParameterBase.EParameterDirection.ReturnValue:
							returnParameter = newParameter;
							break;
						case DexihParameterBase.EParameterDirection.ResultReturnValue:
							resultReturnParameter = newParameter;
							break;
							
					}
				}

				// var returnParameter = TargetDatalinkColumn == null ? null : new ParameterOutputColumn("return", TargetDatalinkColumn.GetTableColumn(null));

				Parameters parameters = new Parameters() { 
					Inputs = inputs, 
					Outputs = outputs, 
					ResultInputs = resultInputs, 
					ResultOutputs = resultOutputs, 
					ReturnParameter = returnParameter,
					ResultReturnParameter = resultReturnParameter
				};
				
//				var inputsArray = inputs.ToArray();
//				var outputsArray = outputs.ToArray();

				TransformFunction function;

				if (!string.IsNullOrEmpty(FunctionClassName))
				{
                    var genericType = GenericTypeCode == null ? null : DataType.GetType(GenericTypeCode.Value);
					function = Functions.GetFunction(FunctionClassName, FunctionMethodName, FunctionAssemblyName).GetTransformFunction(genericType, parameters, globalVariables);
				}
				else
				{
					var generatedClass = CreateFunctionCode(parameters, hub, createConsoleSample);

					var syntaxTree = CSharpSyntaxTree.ParseText(generatedClass);

					var references = new MetadataReference[]
					{
						MetadataReference.CreateFromFile(typeof(object).GetTypeInfo().Assembly.Location),
						MetadataReference.CreateFromFile(typeof(DictionaryBase).GetTypeInfo().Assembly.Location)
					};

					var compilation = CSharpCompilation.Create("Function" + Guid.NewGuid() + ".dll",
						syntaxTrees: new[] {syntaxTree},
						references: references,
						options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

					var message = new StringBuilder();

					using (var ms = new MemoryStream())
					{
						logger?.LogTrace($"GetFunctionMethod, pre-compile  elapse: {timer.Elapsed}");
						var result = compilation.Emit(ms);
						logger?.LogTrace($"GetFunctionMethod, post-compile elapse: {timer.Elapsed}");

						if (!result.Success)
						{
							var failures = result.Diagnostics.Where(diagnostic =>
								diagnostic.IsWarningAsError ||
								diagnostic.Severity == DiagnosticSeverity.Error);

							foreach (var diagnostic in failures)
							{
								message.AppendFormat("{0}: {1}", diagnostic.Id, diagnostic.GetMessage());
							}

							throw new RepositoryException($"Invalid custom function: {message}.");
						}
						else
						{

							ms.Seek(0, SeekOrigin.Begin);

							var folderPath = Path.GetDirectoryName(GetType().GetTypeInfo().Assembly.Location);
							var loader = new AssemblyLoader(folderPath);
							var assembly = loader.LoadFromStream(ms);

							function = new TransformFunction();

							var mappingFunction = assembly.GetType("Program");
							function.ObjectReference = Activator.CreateInstance(mappingFunction);
							function.FunctionMethod = new TransformMethod(mappingFunction.GetMethod("CustomFunction"));
							function.ResetMethod = new TransformMethod(mappingFunction.GetMethod("Reset"));
							// function.ReturnType = ReturnType;
						}
					}

				}

				// if the function has an arrayparamters property then set it.  This give the function access to the 
				// columns that are used, and is specifically used by the column_to_rows function.

				var parameterArray = (ParameterArray) parameters.Inputs.FirstOrDefault(c => c is ParameterArray);
				
				
				var arrayParameterProperty = function.ObjectReference.GetType().GetProperties()
					.SingleOrDefault(c => c.Name == "ArrayParameters");
				if (arrayParameterProperty != null && parameterArray != null)
				{
					arrayParameterProperty.SetValue(function.ObjectReference, parameterArray.Parameters);
				}


//				function.Inputs = inputsArray;
//				function.Outputs = outputsArray;
//				
//				if (TargetDatalinkColumn != null)
//				{
//					function.TargetColumn = TargetDatalinkColumn.GetTableColumn(null);
//				}

				function.OnError = OnError;
				function.OnNull = OnNull;
				function.NotCondition = NotCondition;
				function.InvalidAction = InvalidAction;

				return (function, parameters);

			}
			catch (RepositoryException)
			{
				throw;
			}
            catch (Exception ex)
            {
                throw new RepositoryException($"Function did not compile.  {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Generates the function code using the custom code.
        /// </summary>
        /// <returns></returns>
        public string CreateFunctionCode(Parameters parameters, DexihHub hubCache, bool createConsoleSample = false)
        {
	        string functionCode;
	        
	     	// if this is a reusable function, get the code, otherwise use the code already there.   
	        if (hubCache != null && CustomFunctionKey != null)
	        {
		        var customFunction = hubCache.DexihCustomFunctions.SingleOrDefault(c => c.CustomFunctionKey == CustomFunctionKey && c.IsValid);
		        if (customFunction == null)
		        {
			        throw new RepositoryException($"Failed to load the custom function with the key {CustomFunctionKey}.");
		        }

		        functionCode = customFunction.MethodCode;
	        }
	        else
	        {
		        functionCode = FunctionCode;
	        }

	        if (string.IsNullOrEmpty(functionCode))
	        {
		        throw new RepositoryException("The function contains no code.");
	        }

            var code = new StringBuilder();
            code.Append(@"
using System;
using System.Collections;
using System.Collections.Generic;

// This is a sample program used to test a custom function in the data experts integration hub.
// This code can be run as a c# console application.
public class Program
{
    // These variables are cached between row calls and can be used to store data.
    // These are reset in the group/row transform whenever a neew group is encountered.
	static int intValue;
	static double doubleValue;
	static string stringValue;
	static Dictionary<string, object> cache = new Dictionary<string, object>();

    //This is a test function.
	public static void Main()
	{
        //Input Parameters
$InputParameters

        //Output Parameters
$OutputParameters

        //call the custom function.
$TestFunction

        //write out results to the console.
$WriteResults
    }

    public static $FunctionReturn CustomFunction($Parameters)
    {
        // start: code below here goes into the custom function dialog.

$FunctionCode

        // end: end of code to go into the custom function dialog.
    }

	// The reset function is called when a new group starts on an aggregate function
    public static bool Reset()
    {
        intValue = 0;
        doubleValue = 0;
        stringValue = null;
        cache.Clear();
        return true;
    }

	// helper for getting dictionary values
	static T GetValue<T>(string name)
	{
		if(cache.ContainsKey(name)) 
		{
			return (T) cache[name];
		}
		else 
		{
			return default(T);
		}
	}

	// helper for setting dictionary values
	static void SetValue<T>(string name, T value)
	{
		if(cache.ContainsKey(name)) 
		{
			cache[name] = value;
		}
		else 
		{
			cache.Add(name, value);
		}
	}
}
                    ");

	        var tabbedCode = "\t\t" + functionCode.Replace("\n", "\n\t\t");
	        
            code.Replace("$FunctionCode", tabbedCode);
            code.Replace("$FunctionReturn", parameters.ReturnParameter.DataType.ToString());

            if (createConsoleSample)
            {
                var testFunction = new StringBuilder();
                var returnName = "returnValue";
	            var returnColumn = TargetDatalinkColumn?.GetTableColumn(null);
	            if (returnColumn != null && !string.IsNullOrEmpty(returnColumn.Name))
	            {
		            returnName = returnColumn.Name;
	            }

	            testFunction.Append("\t\t" + returnName + " = ");
                testFunction.Append("CustomFunction(");
	            var p = DexihFunctionParameters.OrderBy(c => c.Position)
		            .Where(c => c.Direction == DexihParameterBase.EParameterDirection.Input).Select(c => c.ParameterName)
		            .ToList();
	            p.AddRange(DexihFunctionParameters.OrderBy(c => c.Position).Where(c => c.Direction == DexihParameterBase.EParameterDirection.Output).Select(c => "out " + c.ParameterName));
	            
	            testFunction.Append(string.Join(", ", p));
                testFunction.Append(");");
                code.Replace("$TestFunction", testFunction.ToString());

                var inputParameters = new StringBuilder();
                foreach (var inputParameter in DexihFunctionParameters.OrderBy(c => c.Position).Where(c => c.Direction == DexihParameterBase.EParameterDirection.Input))
                {
                    inputParameters.Append("\t\t" + inputParameter.DataType + " " + inputParameter.ParameterName + " = ");

	                var parameter = parameters.Inputs?.SingleOrDefault(c => c.Name == inputParameter.ParameterName);
	                if (parameter != null)
	                {
		                var basicType = GetBasicType(inputParameter.DataType);
		                switch (basicType)
		                {
			                case EBasicType.Unknown:
			                case EBasicType.String:
			                case EBasicType.Date:
			                case EBasicType.Time:
			                case EBasicType.Binary:
				                inputParameters.AppendLine("\"" + parameter.Value + "\";");
				                break;
			                case EBasicType.Numeric:
			                case EBasicType.Boolean:
				                inputParameters.AppendLine(parameter.Value + ";");
				                break;
			                default:
				                throw new ArgumentOutOfRangeException();
		                }
	                }
	                else
	                {
		                switch (inputParameter.DataType)
		                {
			                case ETypeCode.Byte:
			                case ETypeCode.SByte:
			                case ETypeCode.UInt16:
			                case ETypeCode.UInt32:
			                case ETypeCode.UInt64:
			                case ETypeCode.Int16:
			                case ETypeCode.Int32:
			                case ETypeCode.Int64:
				                inputParameters.AppendLine("1;");
				                break;
			                case ETypeCode.Decimal:
			                case ETypeCode.Double:
			                case ETypeCode.Single:
				                inputParameters.AppendLine("1.1;");
				                break;
			                case ETypeCode.Text:
			                case ETypeCode.String:
			                case ETypeCode.Unknown:
				                inputParameters.AppendLine("\"sample\";");
				                break;
			                case ETypeCode.Json:
				                inputParameters.AppendLine("\"{ test: \\\"testValue\\\" }\";");
				                break;
			                case ETypeCode.Xml:
				                inputParameters.AppendLine("\"<test>testValue</test>\";");
				                break;
			                case ETypeCode.Boolean:
				                inputParameters.AppendLine("false;");
				                break;
			                case ETypeCode.DateTime:
				                inputParameters.AppendLine("DateTime.Now;");
				                break;
			                case ETypeCode.Time:
				                inputParameters.AppendLine("\"" + TimeSpan.FromDays(1) + "\";");
				                break;
			                case ETypeCode.Guid:
				                inputParameters.AppendLine("\"" + Guid.NewGuid() + "\";");
				                break;
			                default:
				                throw new ArgumentOutOfRangeException();
		                }
	                }
                }
                code.Replace("$InputParameters", inputParameters.ToString());

                var outputParameters = new StringBuilder();
                var writeResults = new StringBuilder();

				outputParameters.AppendLine("\t\t" + parameters.ReturnParameter.DataType + " " + returnName + ";");
				writeResults.AppendLine("\t\tConsole.WriteLine(\"" + returnName + " = {0}\", " + returnName + ");");

                foreach (var outputParameter in DexihFunctionParameters.OrderBy(c => c.Position).Where(c => c.Direction == DexihParameterBase.EParameterDirection.Output))
                {
                    outputParameters.AppendLine("\t\t" + outputParameter.DataType + " " + outputParameter.ParameterName + ";");
                    testFunction.Append("out " + outputParameter.ParameterName + ", ");
                    writeResults.AppendLine("\t\tConsole.WriteLine(\"" + outputParameter.ParameterName + " = {0}\", " + outputParameter.ParameterName + ");");
                }

                code.Replace("$OutputParameters", outputParameters.ToString());
                code.Replace("$WriteResults", writeResults.ToString());


                foreach (var outputParameter in DexihFunctionParameters.OrderBy(c => c.Position).Where(c => c.Direction == DexihParameterBase.EParameterDirection.Output))
                {
                    outputParameters.AppendLine("\t\t" + outputParameter.DataType + " " + outputParameter.ParameterName + ";");
                    testFunction.Append("out " + outputParameter.ParameterName + ", ");
                }
            }
            else
            {
                code.Replace("$TestFunction", "");
                code.Replace("$InputParameters", "");
                code.Replace("$OutputParameters", "");
                code.Replace("$WriteResults", "");

            }


			var parameterString = "";
			foreach (var t in DexihFunctionParameters.OrderBy(c => c.Position).Where(c=>c.Direction == DexihParameterBase.EParameterDirection.Input))
			{
				var addArray = "";
				if (t.Rank > 0) addArray = "[]";
				parameterString += t.DataType + addArray + " " + t.ParameterName + ",";
			}

			foreach (var t in DexihFunctionParameters.OrderBy(c => c.Position).Where(c=>c.Direction == DexihParameterBase.EParameterDirection.Output))
			{
				var addArray = "";
				if (t.Rank > 0) addArray = "[]";
				parameterString += "out " + t.DataType + addArray + " " + t.ParameterName + ",";
			}

            if (parameterString != "") //remove last comma
                parameterString = parameterString.Substring(0, parameterString.Length - 1);

            code.Replace("$Parameters", parameterString);

            return code.ToString();
        }
    }
}


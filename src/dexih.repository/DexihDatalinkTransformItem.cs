using System;
using System.Collections.Generic;

using System.ComponentModel.DataAnnotations.Schema;

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
using System.Text.Json.Serialization;
using dexih.functions.Parameter;
using dexih.transforms;
using dexih.transforms.Mapping;
using Microsoft.Extensions.Logging;
using static dexih.functions.Query.SelectColumn;
using Dexih.Utils.DataType;
using MessagePack;

namespace dexih.repository
{
	[MessagePackObject]
	public class DexihDatalinkTransformItem : DexihHubNamedEntity
	{


		public DexihDatalinkTransformItem() => DexihFunctionParameters = new HashSet<DexihFunctionParameter>();



        [Key(7)]
        [CopyParentCollectionKey]
		public long DatalinkTransformKey { get; set; }

        [Key(8)]
        public int Position { get; set; }

        [Key(9)]
        public ETransformItemType TransformItemType { get; set; }

        [Key(10)]
        public long? TargetDatalinkColumnKey { get; set; }

        [Key(11)]
        public long? SourceDatalinkColumnKey { get; set; }

        [Key(12)]
        public long? JoinDatalinkColumnKey { get; set; }

        [Key(13)]
        public long? FilterDatalinkColumnKey { get; set; }

        [Key(14)]
        public string SourceValue { get; set; }

        [Key(15)]
        public string JoinValue { get; set; }

        [Key(16)]
        public string FilterValue { get; set; }

        [Key(17)]
        public string FunctionClassName { get; set; }

        [Key(18)]
        public string FunctionAssemblyName { get; set; }

        [Key(19)]
        public string FunctionMethodName { get; set; }

        [Key(20)]
        public bool IsGeneric { get; set; }

        [Key(21)]
        public ETypeCode? GenericTypeCode { get; set; }

        [Key(22)]
        public EFunctionCaching FunctionCaching { get; set; }

        [Key(23)]
        public long? CustomFunctionKey { get; set; }

        [Key(24)]
        public Sort.EDirection? SortDirection { get; set; }

        [Key(25)]
        public ECompare? FilterCompare { get; set; }

        [Key(26)]
        public EAggregate? Aggregate { get; set; }

        [Key(27)]
        public ESeriesGrain? SeriesGrain { get; set; }

        [Key(28)]
        public bool SeriesFill { get; set; }

        [Key(29)]
        public string SeriesStart { get; set; }

        [Key(30)]
        public string SeriesFinish { get; set; }

        [Key(31)]
        public string FunctionCode { get; set; }

        [Key(32)]
        public string FunctionResultCode { get; set; }

        [Key(33)]
        public EErrorAction OnError { get; set; }

        [Key(34)]
        public EErrorAction OnNull { get; set; }

        [Key(35)]
        public bool NotCondition { get; set; }

        [Key(36)]
        public TransformFunction.EInvalidAction InvalidAction { get; set; }

        [Key(37)]
        [NotMapped, CopyIgnore]
		public EntityStatus EntityStatus { get; set; }

        [Key(38)]
        public ICollection<DexihFunctionParameter> DexihFunctionParameters { get; set; }

		[JsonIgnore, CopyIgnore, IgnoreMember]
		public virtual DexihDatalinkTransform Dt { get; set; }

        [Key(39)]
        [CopyIgnore]
		public virtual DexihDatalinkColumn SourceDatalinkColumn { get; set; }

        [Key(40)]
        [CopyIgnore]
		public virtual DexihDatalinkColumn TargetDatalinkColumn { get; set; }

        [Key(41)]
        [CopyIgnore]
		public virtual DexihDatalinkColumn JoinDatalinkColumn { get; set; }

        [Key(42)]
        [CopyIgnore]
		public virtual DexihDatalinkColumn FilterDatalinkColumn { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public virtual DexihCustomFunction CustomFunction { get; set; }

        public override void ResetKeys()
        {
	        Key = 0;
	        SourceDatalinkColumn?.ResetKeys();
	        TargetDatalinkColumn?.ResetKeys();
	        JoinDatalinkColumn?.ResetKeys();
	        FilterDatalinkColumn?.ResetKeys();
            
	        foreach (var parameter in DexihFunctionParameters)
	        {
		        parameter.ResetKeys();
	        }
        }
        
		private Parameter ConvertParameter(DexihFunctionParameterBase parameter, EParameterDirection direction)
		{

			var column = parameter.DatalinkColumn?.GetTableColumn(null);

			switch (direction)
			{
				case EParameterDirection.Input:
				case EParameterDirection.ResultInput:
					if (column != null)
					{
						return new ParameterColumn(parameter.Name, parameter.DataType, parameter.Rank, column);
					}
					else
					{
						return new ParameterValue(parameter.Name, parameter.DataType, parameter.Value);
					}
				case EParameterDirection.Join:
					if (column != null)
					{
						return new ParameterJoinColumn(parameter.Name, column);
					}
					else
					{
						return new ParameterValue(parameter.Name, parameter.DataType, parameter.Value);
					}
				default:
					return new ParameterOutputColumn(parameter.Name, parameter.DataType, parameter.Rank, column);				
			}
		}
		
        /// <summary>
        /// Creates a reference to a compiled version of the mapping function.
        /// </summary>
        /// <returns></returns>
        public (TransformFunction function, Parameters parameters) CreateFunctionMethod(DexihHub hub, GlobalSettings globalSettings, bool createConsoleSample = false, ILogger logger = null)
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
				var returnParameters = new List<Parameter>();
				var resultReturnParameters = new List<Parameter>();

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

							newParameter = new ParameterArray(parameter.Name, parameter.DataType, parameter.Rank, arrayParameters);
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
						case EParameterDirection.Input:
                        case EParameterDirection.Join:
                            inputs.Add(newParameter);
							break;
						case EParameterDirection.Output:
							outputs.Add(newParameter);
							break;
						case EParameterDirection.ResultInput:
							resultInputs.Add(newParameter);
							break;
						case EParameterDirection.ResultOutput:
							resultOutputs.Add(newParameter);
							break;
						case EParameterDirection.ReturnValue:
							returnParameters.Add(newParameter);
							break;
						case EParameterDirection.ResultReturnValue:
							resultReturnParameters.Add(newParameter);
							break;
                    }
                }

				// var returnParameter = TargetDatalinkColumn == null ? null : new ParameterOutputColumn("return", TargetDatalinkColumn.GetTableColumn(null));

				Parameters parameters = new Parameters() { 
					Inputs = inputs, 
					Outputs = outputs, 
					ResultInputs = resultInputs, 
					ResultOutputs = resultOutputs, 
					ReturnParameters = returnParameters,
					ResultReturnParameters = resultReturnParameters,
				};
				
//				var inputsArray = inputs.ToArray();
//				var outputsArray = outputs.ToArray();

				TransformFunction function;

				if (!string.IsNullOrEmpty(FunctionClassName))
				{
                    var genericType = GenericTypeCode == null ? null : DataType.GetType(GenericTypeCode.Value);
					function = Functions.GetFunction(FunctionClassName, FunctionMethodName, FunctionAssemblyName).GetTransformFunction(genericType, parameters, globalSettings);
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

							var loader = new AssemblyLoader();
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
		        var customFunction = hubCache.DexihCustomFunctions.SingleOrDefault(c => c.Key == CustomFunctionKey && c.IsValid);
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
    // These are reset in the group/row transform whenever a new group is encountered.
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

            var functionCode2 = functionCode.TrimStart();
            if (functionCode2[0] == '=')
            {
	            functionCode2 = "return " + functionCode2.Substring(1) + ";";
            }
            
	        var tabbedCode = "\t\t" + functionCode2.Replace("\n", "\n\t\t");
	        
            code.Replace("$FunctionCode", tabbedCode);

            if (parameters.ReturnParameters.Count == 0)
            {
	            code.Replace("$FunctionReturn", "void");
            }
            else
            {
	            code.Replace("$FunctionReturn", parameters.ReturnParameters[0].DataType.ToString());    
            }
            

            if (createConsoleSample)
            {
                var testFunction = new StringBuilder();
                var returnName = "returnValue";
	            var returnColumn = TargetDatalinkColumn?.GetTableColumn(null);
	            if (returnColumn != null && !string.IsNullOrEmpty(returnColumn.Name))
	            {
		            returnName = returnColumn.Name;
	            }

	            testFunction.Append("\t\t");
	            if (parameters.ReturnParameters.Count > 0)
	            {
		            testFunction.Append(returnName + " = ");
	            }
	            
                testFunction.Append("CustomFunction(");
	            var p = DexihFunctionParameters.OrderBy(c => c.Position)
		            .Where(c => c.Direction == EParameterDirection.Input).Select(c => c.Name)
		            .ToList();
	            p.AddRange(DexihFunctionParameters.OrderBy(c => c.Position).Where(c => c.Direction == EParameterDirection.Output).Select(c => "out " + c.Name));
	            
	            testFunction.Append(string.Join(", ", p));
                testFunction.Append(");");
                code.Replace("$TestFunction", testFunction.ToString());

                var inputParameters = new StringBuilder();
                foreach (var inputParameter in DexihFunctionParameters.OrderBy(c => c.Position).Where(c => c.Direction == EParameterDirection.Input))
                {
                    inputParameters.Append("\t\t" + inputParameter.DataType + AddRank(inputParameter.Rank) + " " + inputParameter.Name + " = ");

	                var parameter = parameters.Inputs?.SingleOrDefault(c => c.Name == inputParameter.Name);
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
				                inputParameters.AppendLine(AddRankValue("\"" + parameter.Value + "\"", inputParameter.Rank));
				                break;
			                case EBasicType.Numeric:
			                case EBasicType.Boolean:
				                inputParameters.AppendLine(AddRankValue(parameter.Value , inputParameter.Rank));
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
				                inputParameters.AppendLine(AddRankValue("1", inputParameter.Rank));
				                break;
			                case ETypeCode.Decimal:
			                case ETypeCode.Double:
			                case ETypeCode.Single:
				                inputParameters.AppendLine(AddRankValue("1.1", inputParameter.Rank));
				                break;
			                case ETypeCode.Text:
			                case ETypeCode.String:
			                case ETypeCode.Unknown:
				                inputParameters.AppendLine(AddRankValue("\"sample\"", inputParameter.Rank));
				                break;
			                case ETypeCode.Json:
				                inputParameters.AppendLine(AddRankValue("\"{ test: \\\"testValue\\\" }\"", inputParameter.Rank));
				                break;
			                case ETypeCode.Xml:
				                inputParameters.AppendLine(AddRankValue("\"<test>testValue</test>\"", inputParameter.Rank));
				                break;
			                case ETypeCode.Boolean:
				                inputParameters.AppendLine(AddRankValue("false", inputParameter.Rank));
				                break;
			                case ETypeCode.DateTime:
				                inputParameters.AppendLine(AddRankValue("DateTime.Now", inputParameter.Rank));
				                break;
			                case ETypeCode.Time:
				                inputParameters.AppendLine(AddRankValue("\"" + TimeSpan.FromDays(1) + "\"", inputParameter.Rank));
				                break;
			                case ETypeCode.Guid:
				                inputParameters.AppendLine(AddRankValue("\"" + Guid.NewGuid() + "\"", inputParameter.Rank));
				                break;
			                default:
				                throw new ArgumentOutOfRangeException();
		                }
	                }
                }
                code.Replace("$InputParameters", inputParameters.ToString());

                var outputParameters = new StringBuilder();
                var writeResults = new StringBuilder();

                if (parameters.ReturnParameters.Count > 0)
                {
	                outputParameters.AppendLine("\t\t" + parameters.ReturnParameters[0].DataType + " " + returnName +
	                                            ";");
	                writeResults.AppendLine("\t\tConsole.WriteLine(\"" + returnName + " = {0}\", " + returnName + ");");
                }
                else
                {
	                writeResults.AppendLine("\t\tConsole.WriteLine(\"No return value specified.\");");
                }

                foreach (var outputParameter in DexihFunctionParameters.OrderBy(c => c.Position).Where(c => c.Direction == EParameterDirection.Output))
                {
                    outputParameters.AppendLine("\t\t" + outputParameter.DataType + " " + outputParameter.Name + ";");
                    testFunction.Append("out " + outputParameter.Name + ", ");
                    writeResults.AppendLine("\t\tConsole.WriteLine(\"" + outputParameter.Name + " = {0}\", " + outputParameter.Name + ");");
                }

                code.Replace("$OutputParameters", outputParameters.ToString());
                code.Replace("$WriteResults", writeResults.ToString());


                foreach (var outputParameter in DexihFunctionParameters.OrderBy(c => c.Position).Where(c => c.Direction == EParameterDirection.Output))
                {
                    outputParameters.AppendLine("\t\t" + outputParameter.DataType + " " + outputParameter.Name + ";");
                    testFunction.Append("out " + outputParameter.Name + ", ");
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
			foreach (var t in DexihFunctionParameters.OrderBy(c => c.Position).Where(c=>c.Direction == EParameterDirection.Input))
			{
				parameterString += t.DataType + AddRank(t.Rank) + " " + t.Name + ",";
			}

			foreach (var t in DexihFunctionParameters.OrderBy(c => c.Position).Where(c=>c.Direction == EParameterDirection.Output))
			{
				parameterString += "out " + t.DataType + AddRank(t.Rank) + " " + t.Name + ",";
			}

            if (parameterString != "") //remove last comma
                parameterString = parameterString.Substring(0, parameterString.Length - 1);

            code.Replace("$Parameters", parameterString);

            return code.ToString();
        }

        private string AddRank(int rank)
        {
	        return rank > 0 ? "[]" : "";
        }

        private string AddRankValue(object value, int rank)
        {
	        return rank == 0 ? value + ";" : "new [] {" + value + "};";
        }
    }
}


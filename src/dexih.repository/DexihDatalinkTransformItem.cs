using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations.Schema;
using Newtonsoft.Json.Converters;
using dexih.functions;
using static dexih.functions.FunctionReference;
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
using Microsoft.EntityFrameworkCore.ValueGeneration.Internal;
using Microsoft.Extensions.Logging;
using static dexih.functions.Query.SelectColumn;

namespace dexih.repository
{
	public class DexihDatalinkTransformItem : DexihBaseEntity
	{
		[JsonConverter(typeof(StringEnumConverter))]
		public enum ETransformItemType
		{
			BuiltInFunction, CustomFunction, ColumnPair, JoinPair, Sort, Column, FilterPair, AggregatePair
		}

		public DexihDatalinkTransformItem() => DexihFunctionParameters = new HashSet<DexihFunctionParameter>();

		[JsonIgnore, CopyIgnore]
		public long HubKey { get; set; }

		[CopyCollectionKey((long)0, true)]
		public long DatalinkTransformItemKey { get; set; }

		[CopyParentCollectionKey]
		public long DatalinkTransformKey { get; set; }

		public int Position { get; set; }

		[NotMapped]
		public ETransformItemType TransformItemType { get; set; }

		[JsonIgnore, CopyIgnore]
		public string TransformItemTypeString
		{
			get => TransformItemType.ToString();
			set => TransformItemType = (ETransformItemType)Enum.Parse(typeof(ETransformItemType), value);
		}

		public long? TargetDatalinkColumnKey { get; set; }
		public long? SourceDatalinkColumnKey { get; set; }
		public long? JoinDatalinkColumnKey { get; set; }
		public long? FilterDatalinkColumnKey { get; set; }

		public string JoinValue { get; set; }
		public string FilterValue { get; set; }

		public string FunctionClassName { get; set; }
		public string FunctionAssemblyName { get; set; }
		public string FunctionMethodName { get; set; }

		public long? CustomFunctionKey { get; set; }

        [NotMapped]
		public Sort.EDirection? SortDirection { get; set; }
		[JsonIgnore, CopyIgnore]
		public string SortDirectionString
		{
			get => SortDirection.ToString();
			set
			{
				if (string.IsNullOrEmpty(value))
					SortDirection = null;
				else
					SortDirection = (Sort.EDirection)Enum.Parse(typeof(Sort.EDirection), value);
			}
		}

		[NotMapped]
		public Filter.ECompare? FilterCompare { get; set; }

		[JsonIgnore, CopyIgnore]
		public string FilterCompareString
		{
			get => FilterCompare.ToString();
			set
			{
				if (string.IsNullOrEmpty(value))
					FilterCompare = null;
				else
					FilterCompare = (Filter.ECompare)Enum.Parse(typeof(Filter.ECompare), value);
			}
		}

        [NotMapped]
        public EAggregate? Aggregate { get; set; }

        [JsonIgnore, CopyIgnore]
        public string AggregateString
        {
            get => Aggregate.ToString();
            set
            {
                if (string.IsNullOrEmpty(value))
                    Aggregate = null;
                else
                    Aggregate = (EAggregate)Enum.Parse(typeof(EAggregate), value);
            }
        }

        [NotMapped]
		public ETypeCode ReturnType { get; set; }
		[JsonIgnore, CopyIgnore]
		public string ReturnTypeString
		{
			get => ReturnType.ToString();
			set => ReturnType = (ETypeCode)Enum.Parse(typeof(ETypeCode), value);
		}
		public string FunctionCode { get; set; }
		public string FunctionResultCode { get; set; }

		[NotMapped]
		public EErrorAction OnError { get; set; }
		[JsonIgnore, CopyIgnore]
		public string OnErrorString
		{
			get => OnError.ToString();
			set => OnError = (EErrorAction)Enum.Parse(typeof(EErrorAction), value);
		}

		[NotMapped]
		public EErrorAction OnNull { get; set; }
		[JsonIgnore, CopyIgnore]
		public string OnNullString
		{
			get => OnNull.ToString();
			set => OnNull = (EErrorAction)Enum.Parse(typeof(EErrorAction), value);
		}
		public bool NotCondition { get; set; }

		[NotMapped]
		public TransformFunction.EInvalidAction InvalidAction { get; set; }
		[JsonIgnore, CopyIgnore]
		public string InvalidActionString
		{
			get => InvalidAction.ToString();
			set => InvalidAction = (TransformFunction.EInvalidAction)Enum.Parse(typeof(TransformFunction.EInvalidAction), value);
		}

		[NotMapped, CopyIgnore]
		public EntityStatus EntityStatus { get; set; }

		public virtual ICollection<DexihFunctionParameter> DexihFunctionParameters { get; set; }

		[JsonIgnore, CopyIgnore]
		public virtual DexihDatalinkTransform Dt { get; set; }

		[CopyReference]
		public virtual DexihDatalinkColumn SourceDatalinkColumn { get; set; }

		[CopyReference]
		public virtual DexihDatalinkColumn TargetDatalinkColumn { get; set; }

		[CopyReference]
		public virtual DexihDatalinkColumn JoinDatalinkColumn { get; set; }

		[CopyReference]
		public virtual DexihDatalinkColumn FilterDatalinkColumn { get; set; }

        // public virtual DexihStandardFunction StandardFunction { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihCustomFunction CustomFunction { get; set; }

        /// <summary>
        /// Creates a reference to a compiled version of the mapping function.
        /// </summary>
        /// <returns></returns>
        public TransformFunction CreateFunctionMethod(DexihHub hub, bool createConsoleSample = false, ILogger logger = null)
		{
			try
			{
				var timer = Stopwatch.StartNew();
				logger?.LogTrace($"GetFunctionMethod, started.");

				if (TransformItemType != ETransformItemType.CustomFunction && TransformItemType != ETransformItemType.BuiltInFunction)
				{
					throw new RepositoryException("The datalink transform item is not a custom function");
				}

				//create the input & output parameters
				var inputs = new List<Parameter>();
				var outputs = new List<Parameter>();

				foreach (var parameter in DexihFunctionParameters)
				{
					var newParameter = new Parameter()
					{
						Column = parameter.DatalinkColumn?.GetTableColumn(),
						DataType = parameter.DataType,
						IsArray = parameter.IsArray,
						IsColumn = parameter.DatalinkColumn != null,
						Name = parameter.ParameterName,
					};

					try
					{
						newParameter.SetValue(parameter.Value);
					}
					catch (Exception ex)
					{
#if DEBUG
						throw new RepositoryException($"Failed to set parameter {parameter.ParameterName} with value {parameter.Value}.  {ex.Message}", ex);
#else
						throw new RepositoryException($"Failed to set parameter {parameter.ParameterName}.  {ex.Message}", ex);
#endif
					}

                    if (parameter.Direction == DexihParameterBase.EParameterDirection.Input)
                        inputs.Add(newParameter);
                    else
                        outputs.Add(newParameter);
                }

                var inputsArray = inputs.ToArray();
                var outputsArray = outputs.ToArray();
                
				TransformFunction function;

				if (!string.IsNullOrEmpty(FunctionClassName))
				{
					function = Functions.GetFunction(FunctionClassName, FunctionMethodName, FunctionAssemblyName).GetTransformFunction();
                }
                else
				{
					var generatedClass = CreateFunctionCode(inputsArray, outputsArray, hub, createConsoleSample);
                    
                    var syntaxTree = CSharpSyntaxTree.ParseText(generatedClass);

                    var references = new MetadataReference[]
                    {
                        MetadataReference.CreateFromFile(typeof(object).GetTypeInfo().Assembly.Location),
                        MetadataReference.CreateFromFile(typeof(Hashtable).GetTypeInfo().Assembly.Location)
                    };

                    var compilation = CSharpCompilation.Create("Function" + Guid.NewGuid() + ".dll",
                    syntaxTrees: new[] { syntaxTree },
                    references: references,
                    options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

                    var message = new StringBuilder();

                    using (var ms = new MemoryStream())
                    {
	                    logger?.LogTrace($"GetFunctionMethod, pre-compile  elspase: {timer.Elapsed}");
                        var result = compilation.Emit(ms);
	                    logger?.LogTrace($"GetFunctionMethod, post-compile elspase: {timer.Elapsed}");

                        if (!result.Success)
                        {
                            var failures = result.Diagnostics.Where(diagnostic =>
                                diagnostic.IsWarningAsError ||
                                diagnostic.Severity == DiagnosticSeverity.Error);

                            foreach (var diagnostic in failures)
                            {
                                message.AppendFormat("{0}: {1}", diagnostic.Id, diagnostic.GetMessage());
                            }

                            throw new RepositoryException($"Failed to compile custom function.  {message}.");
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
                            function.FunctionMethod = mappingFunction.GetMethod("CustomFunction");
                            function.ResetMethod = mappingFunction.GetMethod("Reset");
                            function.ReturnType = ReturnType;
                        }
                    }

                }

                function.Inputs = inputsArray;
                function.Outputs = outputsArray;
				if (TargetDatalinkColumn != null)
				{
					function.TargetColumn = TargetDatalinkColumn.GetTableColumn();
				}
                function.OnError = OnError;
                function.OnNull = OnNull;
                function.NotCondition = NotCondition;
                function.InvalidAction = InvalidAction;

                return function;

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
        public string CreateFunctionCode(Parameter[] inputs, Parameter[] outputs, DexihHub hubCache, bool createConsoleSample = false)
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

// This is a sample program used to test a custom function in the data experts integration hub.
// This code can be run as a c# console application.
public class Program
{
    // These variables are cached between row calls and can be used to store data.
    // These are reset in the group/row transform whenever a neew group is encountered.
	static int? CacheInt;
	static double? CacheDouble;
	static string CacheString;
	static Hashtable CacheHashtable;

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
        CacheInt = null;
        CacheDouble = null;
        CacheString = null;
        CacheHashtable = null;
        return true;
    }
}
                    ");

	        var tabbedCode = "\t\t" + functionCode.Replace("\n", "\n\t\t");
	        
            code.Replace("$FunctionCode", tabbedCode);
            code.Replace("$FunctionReturn", ReturnType.ToString());

            if (createConsoleSample)
            {
                var testFunction = new StringBuilder();
                var returnName = "returnValue";
	            var returnColumn = TargetDatalinkColumn?.GetTableColumn();
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
                foreach (var inputParameter in DexihFunctionParameters.OrderBy(c => c.Position).Where(c => c.Direction == DexihFunctionParameter.EParameterDirection.Input))
                {
                    inputParameters.Append("\t\t" + inputParameter.DataType + " " + inputParameter.ParameterName + " = ");

	                var parameter = inputs?.SingleOrDefault(c => c.Name == inputParameter.ParameterName);
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

				outputParameters.AppendLine("\t\t" + ReturnType + " " + returnName + ";");
				writeResults.AppendLine("\t\tConsole.WriteLine(\"" + returnName + " = {0}\", " + returnName + ");");

                foreach (var outputParameter in DexihFunctionParameters.OrderBy(c => c.Position).Where(c => c.Direction == DexihFunctionParameter.EParameterDirection.Output))
                {
                    outputParameters.AppendLine("\t\t" + outputParameter.DataType + " " + outputParameter.ParameterName + ";");
                    testFunction.Append("out " + outputParameter.ParameterName + ", ");
                    writeResults.AppendLine("\t\tConsole.WriteLine(\"" + outputParameter.ParameterName + " = {0}\", " + outputParameter.ParameterName + ");");
                }

                code.Replace("$OutputParameters", outputParameters.ToString());
                code.Replace("$WriteResults", writeResults.ToString());


                foreach (var outputParameter in DexihFunctionParameters.OrderBy(c => c.Position).Where(c => c.Direction == DexihFunctionParameter.EParameterDirection.Output))
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
			foreach (var t in DexihFunctionParameters.OrderBy(c => c.Position).Where(c=>c.Direction == DexihFunctionParameter.EParameterDirection.Input))
			{
				var addArray = "";
				if (t.IsArray) addArray = "[]";
				parameterString += t.DataType + addArray + " " + t.ParameterName + ",";
			}

			foreach (var t in DexihFunctionParameters.OrderBy(c => c.Position).Where(c=>c.Direction == DexihFunctionParameter.EParameterDirection.Output))
			{
				var addArray = "";
				if (t.IsArray) addArray = "[]";
				parameterString += "out " + t.DataType + addArray + " " + t.ParameterName + ",";
			}

            if (parameterString != "") //remove last comma
                parameterString = parameterString.Substring(0, parameterString.Length - 1);

            code.Replace("$Parameters", parameterString);

            return code.ToString();
        }
    }
}


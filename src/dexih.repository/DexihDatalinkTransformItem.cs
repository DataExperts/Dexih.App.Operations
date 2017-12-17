using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations.Schema;
using Newtonsoft.Json.Converters;
using dexih.functions;
using static dexih.functions.Function;
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
using Microsoft.Extensions.Logging;

namespace dexih.repository
{
	public class DexihDatalinkTransformItem : DexihBaseEntity
	{
		[JsonConverter(typeof(StringEnumConverter))]
		public enum ETransformItemType
		{
			BuiltInFunction, CustomFunction, ColumnPair, JoinPair, Sort, Column
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
			get { return TransformItemType.ToString(); }
			set { TransformItemType = (ETransformItemType)Enum.Parse(typeof(ETransformItemType), value); }
		}

		public long? TargetDatalinkColumnKey { get; set; }
		public long? SourceDatalinkColumnKey { get; set; }
		public long? JoinDatalinkColumnKey { get; set; }

		public string JoinValue { get; set; }

		public long? StandardFunctionKey { get; set; }

		[NotMapped]
		public Sort.EDirection? SortDirection { get; set; }
		[JsonIgnore, CopyIgnore]
		public string SortDirectionString
		{
			get { return SortDirection.ToString(); }
			set
			{
				if (string.IsNullOrEmpty(value))
					SortDirection = null;
				else
					SortDirection = (Sort.EDirection)Enum.Parse(typeof(Sort.EDirection), value);
			}
		}
		[NotMapped]
		public ETypeCode ReturnType { get; set; }
		[JsonIgnore, CopyIgnore]
		public string ReturnTypeString
		{
			get { return ReturnType.ToString(); }
			set { ReturnType = (ETypeCode)Enum.Parse(typeof(ETypeCode), value); }
		}
		public string FunctionCode { get; set; }
		public string FunctionResultCode { get; set; }

		[NotMapped]
		public EErrorAction OnError { get; set; }
		[JsonIgnore, CopyIgnore]
		public string OnErrorString
		{
			get { return OnError.ToString(); }
			set { OnError = (EErrorAction)Enum.Parse(typeof(EErrorAction), value); }
		}

		[NotMapped]
		public EErrorAction OnNull { get; set; }
		[JsonIgnore, CopyIgnore]
		public string OnNullString
		{
			get { return OnNull.ToString(); }
			set { OnNull = (EErrorAction)Enum.Parse(typeof(EErrorAction), value); }
		}
		public bool NotCondition { get; set; }

		[NotMapped]
		public EInvalidAction InvalidAction { get; set; }
		[JsonIgnore, CopyIgnore]
		public string InvalidActionString
		{
			get { return InvalidAction.ToString(); }
			set { InvalidAction = (EInvalidAction)Enum.Parse(typeof(EInvalidAction), value); }
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

		public virtual DexihStandardFunction StandardFunction { get; set; }

		/// <summary>
		/// Creates a reference to a compiled version of the mapping function.
		/// </summary>
		/// <returns></returns>
		public Function CreateFunctionMethod(bool createConsoleSample = false, ILogger logger = null)
		{
			try
			{
				var timer = Stopwatch.StartNew();
				logger?.LogTrace($"GetFunctionMethod, started.");

				if (TransformItemType != ETransformItemType.CustomFunction && TransformItemType != DexihDatalinkTransformItem.ETransformItemType.BuiltInFunction)
				{
					throw new RepositoryException("The datalink transform item is not a custom function");
				}

				//create the input & output parameters
				var inputs = new List<Parameter>();
				var outputs = new List<functions.Parameter>();

				foreach (var parameter in DexihFunctionParameters)
				{
					var newParameter = new functions.Parameter()
					{
						Column = parameter.DatalinkColumn?.GetTableColumn(),
						DataType = parameter.Datatype,
						IsArray = parameter.IsArray,
						IsColumn = parameter.DatalinkColumn != null,
						Name = parameter.ParameterName
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

                    if (parameter.Direction == DexihFunctionParameter.EParameterDirection.Input)
                        inputs.Add(newParameter);
                    else
                        outputs.Add(newParameter);
                }

                var Inputs = inputs.ToArray();
                var Outputs = outputs.ToArray();
                Function function;

				if (StandardFunction != null)
                {
                    function = StandardFunctions.GetFunctionReference(StandardFunction.Method);
                }
                else
                {
                    var generatedClass = CreateFunctionCode(Inputs, Outputs, createConsoleSample);
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

                            var folderPath = Path.GetDirectoryName(this.GetType().GetTypeInfo().Assembly.Location);
                            var loader = new AssemblyLoader(folderPath);
                            var assembly = loader.LoadFromStream(ms);

                            function = new Function();

                            var mappingFunction = assembly.GetType("Program");
                            function.ObjectReference = Activator.CreateInstance(mappingFunction);
                            function.FunctionMethod = mappingFunction.GetMethod("CustomFunction");
                            function.ResetMethod = mappingFunction.GetMethod("Reset");
                            function.ReturnType = ReturnType;
                        }
                    }

                }

                function.Inputs = Inputs;
                function.Outputs = Outputs;
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
                throw new RepositoryException($"Failed create the function.  {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Generates the function code using the custom code.
        /// </summary>
        /// <returns></returns>
        public string CreateFunctionCode(Parameter[] inputs, Parameter[] outputs, bool createConsoleSample = false)
        {
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
        // start cut and paste code below here into the custom function dialog.

        $FunctionCode

        // end cut and paste of code into the custom function dialog.
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

            code.Replace("$FunctionCode", FunctionCode);
            code.Replace("$FunctionReturn", ReturnType.ToString());

            if (createConsoleSample)
            {
                var testFunction = new StringBuilder();
                TableColumn targetColumn = null;
                if (TargetDatalinkColumn != null)
                {
                    targetColumn = TargetDatalinkColumn.GetTableColumn();
                    if (targetColumn != null)
                        testFunction.Append(targetColumn.Name + " = ");
                }

                testFunction.Append("CustomFunction(");
                testFunction.Append(string.Join(",", DexihFunctionParameters.OrderBy(c => c.Position).Select(c => c.Direction == DexihFunctionParameter.EParameterDirection.Output ? "out " + c.ParameterName : c.ParameterName).ToArray()));
                testFunction.Append(");");
                code.Replace("$TestFunction", testFunction.ToString());

                var inputParameters = new StringBuilder();
                foreach (var inputParameter in DexihFunctionParameters.OrderBy(c => c.Position).Where(c => c.Direction == DexihFunctionParameter.EParameterDirection.Input))
                {
                    inputParameters.Append(inputParameter.Datatype + " " + inputParameter.ParameterName + " = ");
                    switch (inputParameter.Datatype)
                    {
                        case ETypeCode.Byte:
                        case ETypeCode.SByte:
                        case ETypeCode.UInt16:
                        case ETypeCode.UInt32:
                        case ETypeCode.UInt64:
                        case ETypeCode.Int16:
                        case ETypeCode.Int32:
                        case ETypeCode.Int64:
                            inputParameters.Append("1;");
                            break;
                        case ETypeCode.Decimal:
                        case ETypeCode.Double:
                        case ETypeCode.Single:
                            inputParameters.Append("1.1;");
                            break;
                        case ETypeCode.String:
                            inputParameters.Append("\"sample\";");
                            break;
                        case ETypeCode.Boolean:
                            inputParameters.Append("\"false\";");
                            break;
                        case ETypeCode.DateTime:
                            inputParameters.Append("DateTime.Now;");
                            break;
                        case ETypeCode.Time:
                            inputParameters.Append("\"" + TimeSpan.FromDays(1) + "\";");
                            break;
                        case ETypeCode.Guid:
                            inputParameters.Append("\"" + Guid.NewGuid() + "\";");
                            break;
                        case ETypeCode.Unknown:
                        default:
                            break;
                    }
                }
                code.Replace("$InputParameters", inputParameters.ToString());

                var outputParameters = new StringBuilder();
                var writeResults = new StringBuilder();

                if (targetColumn != null)
                {
                    outputParameters.Append(ReturnType + " " + targetColumn.Name + ";");
                    writeResults.Append("Console.WriteLine(\"" + targetColumn.Name + " = {0}\", " + targetColumn.Name + ");");
                }

                foreach (var outputParameter in DexihFunctionParameters.OrderBy(c => c.Position).Where(c => c.Direction == DexihFunctionParameter.EParameterDirection.Output))
                {
                    outputParameters.AppendLine(outputParameter.Datatype + " " + outputParameter.ParameterName + ";");
                    testFunction.Append("out " + outputParameter.ParameterName + ", ");
                    writeResults.Append("Console.WriteLine(\"" + outputParameter.ParameterName + " = {0}\", " + outputParameter.ParameterName + ");");
                }

                code.Replace("$OutputParameters", outputParameters.ToString());
                code.Replace("$WriteResults", writeResults.ToString());


                foreach (var outputParameter in DexihFunctionParameters.OrderBy(c => c.Position).Where(c => c.Direction == DexihFunctionParameter.EParameterDirection.Output))
                {
                    outputParameters.AppendLine(outputParameter.Datatype + " " + outputParameter.ParameterName + ";");
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
            if (inputs != null)
            {
                foreach (var t in inputs)
                {
                    var addArray = "";
                    if (t.IsArray) addArray = "[]";
                    parameterString += t.DataType + addArray + " " + t.Name + ",";
                }
            }

            if (outputs != null)
            {
                foreach (var t in outputs)
                {
                    var addArray = "";
                    if (t.IsArray) addArray = "[]";
                    parameterString += "out " + t.DataType + addArray + " " + t.Name + ",";
                }
            }

            if (parameterString != "") //remove last comma
                parameterString = parameterString.Substring(0, parameterString.Length - 1);

            code.Replace("$Parameters", parameterString);

            var functionCode = code.ToString();

            return functionCode;
        }
    }
}


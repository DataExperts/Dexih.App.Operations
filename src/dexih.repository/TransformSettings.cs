using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Net.Http;
using System.Runtime.Serialization;
using System.Text.Json;
using System.Text.Json.Serialization;


namespace dexih.repository
{
    /// <summary>
    /// Transform Settings is used to pass various parameters when creating the transforms.
    /// </summary>
    [DataContract]
    public class TransformSettings
    {
        [DataMember(Order = 0)]
        public DexihHubVariable[] HubVariables { get; set; }

        [DataMember(Order = 1)]
        public InputParameterBase[] InputParameters { get; set; }

        [DataMember(Order = 2)]
        public RemoteSettings RemoteSettings { get; set; }

        [JsonIgnore]
        public IHttpClientFactory ClientFactory { get; set; }
        
        public bool HasVariables()
        {
            return HubVariables?.Length > 0 || InputParameters?.Length > 0;
        }

        public object UpdateHubVariable(string value, int rank)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return value;
            }
            
            if (rank == 0)
            {
                return InsertHubVariables(value);
            }

            if (rank == 1)
            {
                var trimmed = value.Trim();
                if (trimmed.StartsWith("{") && trimmed.EndsWith("}"))
                {
                    var name = trimmed.Substring(1, trimmed.Length - 2);
                    var variableValue = GetVariableValue(name);

                    if (variableValue != null)
                    {
                        if (variableValue is JsonElement jsonElement)
                        {
                            if (jsonElement.ValueKind == JsonValueKind.Array)
                            {
                                return jsonElement.EnumerateArray().Select(e =>
                                {
                                    var obj = e.EnumerateObject();
                                    var key = obj.SingleOrDefault(c => c.Name == "key");
                                    return key.Value;
                                }).ToArray();
                            }
                        }

                        if (variableValue is Array array)
                        {
                            if (array.Length == 0)
                            {
                                return new string[0];
                            }
                            else
                            {
                                var values = new List<string>();
                                foreach (var arrayValue in array)
                                {
                                    // if the values are json, then just return the "key" attributes.
                                    if (arrayValue is JsonElement jsonElement1)
                                    {
                                        if (jsonElement1.ValueKind == JsonValueKind.Object)
                                        {
                                            var objectElements = jsonElement1.EnumerateObject();
                                            foreach (var element in objectElements.Where(c => c.Name == "key"))
                                            {
                                                values.Add(element.Value.GetString());
                                            }
                                        }
                                    }
                                    else
                                    {
                                        values.Add(arrayValue.ToString());
                                    }
                                }

                                return values.ToArray();
                            }
                        }
                        else
                        {
                            return new [] { variableValue.ToString()};
                        }

                        // if (variableValue is string variableString)
                        // {
                        //     return new[] {variableString};
                        // }
                    }
                    
                }
            }

            return value;
        }
        
        /// <summary>
        /// Replaces variables with actual values
        /// </summary>
        /// <param name="value">String to replace</param>
        /// <param name="allowSecureVariables">Allow encrypted and environment variables to be used.</param>
        /// <returns></returns>
        public string InsertHubVariables(string value)
        {
            if (string.IsNullOrEmpty(value) || ( HubVariables == null || HubVariables.Length == 0) && (InputParameters == null || InputParameters.Length == 0))
            {
                return value;
            }

            var ignoreNext = false;
            var openStart = -1;
            var previousPos = 0;
            StringBuilder newValue = null;

            for (var pos = 0; pos < value.Length; pos++)
            {
                var character = value[pos];

                if (ignoreNext)
                {
                    ignoreNext = false;
                    continue;
                }

                // backslash is escape character, so ignore next value when one is found.
                if (character == '\\')
                {
                    ignoreNext = true;
                    continue;
                }

                if (openStart == -1 && character == '{')
                {
                    openStart = pos;
                    continue;
                }

                if (openStart >= 0 && character == '}')
                {
                    var name = value.Substring(openStart + 1, pos - openStart - 1);
                    var variableValue = GetVariableValue(name);
                    
                    if (variableValue != null)
                    {
                        if (newValue == null)
                        {
                            newValue = new StringBuilder();
                        }
                            
                        newValue.Append(value.Substring(previousPos, openStart - previousPos));
                        newValue.Append(variableValue);
                        previousPos = pos + 1;
                    }
                    
                    openStart = -1;
                }
            }

            if (newValue == null)
            {
                return value;
            }
            else
            {
                newValue.Append(value.Substring(previousPos));
                return newValue.ToString();
            }
        }

        private object GetVariableValue(string name)
        {
            object variableValue = null;
            var variable = HubVariables?.SingleOrDefault(c => c.IsValid && c.Name == name);
            if (variable != null)
            {
                variableValue = variable.GetValue(RemoteSettings.AppSettings.EncryptionKey,
                    RemoteSettings.SystemSettings.EncryptionIterations);
            }
            else
            {
                var parameter = InputParameters?.SingleOrDefault(c => c.Name == name);
                if (parameter != null)
                {
                    variableValue = parameter.Value;
                }
            }

            return variableValue;
        }
    }
}
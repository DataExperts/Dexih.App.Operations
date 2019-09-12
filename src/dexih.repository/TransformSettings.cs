using System;
using System.Text;
using System.Linq;
using ProtoBuf;

namespace dexih.repository
{
    /// <summary>
    /// Transform Settings is used to pass various parameters when creating the transforms.
    /// </summary>
    [ProtoContract]
    public class TransformSettings
    {
        [ProtoMember(1)]
        public DexihHubVariable[] HubVariables { get; set; }

        [ProtoMember(2)]
        public InputParameterBase[] InputParameters { get; set; }

        [ProtoMember(3)]
        public RemoteSettings RemoteSettings { get; set; }

        public bool HasVariables()
        {
            return HubVariables?.Length > 0;
        }
        
        /// <summary>
        /// Replaces variables with actual values
        /// </summary>
        /// <param name="value">String to replace</param>
        /// <param name="allowSecureVariables">Allow encrypted and environment variables to be used.</param>
        /// <returns></returns>
        public string InsertHubVariables(string value, bool allowSecureVariables)
        {
            if (string.IsNullOrEmpty(value) || HubVariables == null || HubVariables.Length == 0)
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
                    string variableValue = null;
                    var variable = HubVariables?.SingleOrDefault(c => c.Name == name);
                    if (variable != null)
                    {
                        if (!allowSecureVariables && (variable.IsEncrypted || variable.IsEnvironmentVariable))
                        {
                            throw new Exception($"The variable {variable.Name} could not be used as encrypted or environment variables are not available for this parameter.");
                        }

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
    }
}
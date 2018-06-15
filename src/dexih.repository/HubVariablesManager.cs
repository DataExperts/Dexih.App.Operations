using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dexih.repository
{

    /// <summary>
    /// HubVariablesManager is used to substitue hubVariable values into strings
    /// </summary>
    public class HubVariablesManager
    {
        private readonly TransformSettings _transformSettings;
        private readonly IEnumerable<DexihHubVariable> _hubVariables;

        public HubVariablesManager(TransformSettings transformSettings, IEnumerable<DexihHubVariable> hubVariables)
        {
            _transformSettings = transformSettings;
            _hubVariables = hubVariables;
        }

        /// <summary>
        /// Replaces variables with actual values
        /// </summary>
        /// <param name="value">String to replace</param>
        /// <param name="allowSecureVariables">Allow encrypted and environment variables to be used.</param>
        /// <returns></returns>
        public string InsertHubVariables(string value, bool allowSecureVariables)
        {
            if (string.IsNullOrEmpty(value))
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
                    var variable = _hubVariables.SingleOrDefault(c => c.Name == name);
                    if (variable == null)
                    {
                        openStart = -1;
                    }
                    else
                    {
                        if (!allowSecureVariables && (variable.IsEncrypted || variable.IsEnvironmentVariable))
                        {
                            throw new Exception($"The variable {variable.Name} could not be used as encrypted or enviornment variables and not available for this parameter.");
                        }
                        
                        if (newValue == null)
                        {
                            newValue = new StringBuilder();
                        }

                        newValue.Append(value.Substring(previousPos, openStart - previousPos));
                        newValue.Append(variable.GetValue(_transformSettings.RemoteSettings.AppSettings.EncryptionKey, _transformSettings.RemoteSettings.SystemSettings.EncryptionIterations));
                        previousPos = pos + 1;
                        openStart = -1;
                    }
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

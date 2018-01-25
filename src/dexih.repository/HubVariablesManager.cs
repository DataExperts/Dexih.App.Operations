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
        private readonly string _encryptionKey;
        private readonly IEnumerable<DexihHubVariable> _hubVariables;

        public HubVariablesManager(string encryptionKey, IEnumerable<DexihHubVariable> hubVariables)
        {
            _encryptionKey = encryptionKey;
            _hubVariables = hubVariables;
        }

        public string InsertHubVariables(string value)
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
                char character = value[pos];

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
                        if (newValue == null)
                        {
                            newValue = new StringBuilder();
                        }

                        newValue.Append(value.Substring(previousPos, openStart - previousPos));
                        newValue.Append(variable.GetValue(_encryptionKey));
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

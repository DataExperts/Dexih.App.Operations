//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//
//namespace dexih.repository
//{
//    /// <summary>
//    /// HubVariablesManager is used to substitue hubVariable values into strings
//    /// </summary>
//    public class HubVariablesManager
//    {
//        private readonly TransformSettings _transformSettings;
//        private readonly DexihHubVariable[] _hubVariables;
//
//        public HubVariablesManager(TransformSettings transformSettings, DexihHubVariable[] hubVariables)
//        {
//            _transformSettings = transformSettings;
//            _hubVariables = hubVariables;
//        }
//
//        public void UpdateTransformItem(DexihDatalinkTransformItem item, bool allowSecureVariables)
//        {
//            if (_hubVariables == null || _hubVariables.Length == 0)
//            {
//                return;
//            }
//
//            if (!string.IsNullOrEmpty(item.FilterValue)) InsertHubVariables(item.FilterValue, allowSecureVariables);
//            if (!string.IsNullOrEmpty(item.JoinValue)) InsertHubVariables(item.JoinValue, allowSecureVariables);
//            if (!string.IsNullOrEmpty(item.SourceValue)) InsertHubVariables(item.SourceValue, allowSecureVariables);
//            if (!string.IsNullOrEmpty(item.SeriesStart)) InsertHubVariables(item.SeriesStart, allowSecureVariables);
//            if (!string.IsNullOrEmpty(item.SeriesFinish)) InsertHubVariables(item.SeriesFinish, allowSecureVariables);
//
//
//            foreach (var param in item.DexihFunctionParameters)
//            {
//                if (!string.IsNullOrEmpty(param.Value)) InsertHubVariables(param.Value, allowSecureVariables);
//
//                foreach (var arrayParam in param.ArrayParameters)
//                {
//                    if (!string.IsNullOrEmpty(arrayParam.Value))
//                        InsertHubVariables(arrayParam.Value, allowSecureVariables);
//                }
//            }
//        }
//
//        /// <summary>
//        /// Replaces variables with actual values
//        /// </summary>
//        /// <param name="value">String to replace</param>
//        /// <param name="allowSecureVariables">Allow encrypted and environment variables to be used.</param>
//        /// <returns></returns>
//        public string InsertHubVariables(string value, bool allowSecureVariables)
//        {
//            if (string.IsNullOrEmpty(value) || _hubVariables == null || _hubVariables.Length == 0)
//            {
//                return value;
//            }
//
//            var ignoreNext = false;
//            var openStart = -1;
//            var previousPos = 0;
//            StringBuilder newValue = null;
//
//            for (var pos = 0; pos < value.Length; pos++)
//            {
//                var character = value[pos];
//
//                if (ignoreNext)
//                {
//                    ignoreNext = false;
//                    continue;
//                }
//
//                // backslash is escape character, so ignore next value when one is found.
//                if (character == '\\')
//                {
//                    ignoreNext = true;
//                    continue;
//                }
//
//                if (openStart == -1 && character == '{')
//                {
//                    openStart = pos;
//                    continue;
//                }
//
//                if (openStart >= 0 && character == '}')
//                {
//                    var name = value.Substring(openStart + 1, pos - openStart - 1);
//                    var variable = _hubVariables.SingleOrDefault(c => c.Name == name);
//                    if (variable == null)
//                    {
//                        openStart = -1;
//                    }
//                    else
//                    {
//                        if (!allowSecureVariables && (variable.IsEncrypted || variable.IsEnvironmentVariable))
//                        {
//                            throw new Exception(
//                                $"The variable {variable.Name} could not be used as encrypted or environment variables are not available for this parameter.");
//                        }
//
//                        if (newValue == null)
//                        {
//                            newValue = new StringBuilder();
//                        }
//
//                        newValue.Append(value.Substring(previousPos, openStart - previousPos));
//                        newValue.Append(variable.GetValue(_transformSettings.RemoteSettings.AppSettings.EncryptionKey,
//                            _transformSettings.RemoteSettings.SystemSettings.EncryptionIterations));
//                        previousPos = pos + 1;
//                        openStart = -1;
//                    }
//                }
//            }
//
//            if (newValue == null)
//            {
//                return value;
//            }
//            else
//            {
//                newValue.Append(value.Substring(previousPos));
//                return newValue.ToString();
//            }
//        }
//    }
//}
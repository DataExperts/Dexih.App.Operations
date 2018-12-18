﻿using dexih.repository;
using Dexih.Utils.MessageHelpers;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;

namespace dexih.operations
{
    public class ResponseMessage : RemoteMessage
    {
        public ResponseMessage(string securityToken, string messageId, ReturnValue<JToken> returnMessage)
        {
            MessageId = messageId;
            SecurityToken = securityToken;
            Method = "Response";

            Success = returnMessage.Success;
            Message = returnMessage.Message;
            Value = returnMessage.Value;
            Exception = returnMessage.Exception;
        }
    }
    public class RemoteMessage : ReturnValue<JToken>
    {
        public RemoteMessage()
        {
            Success = true;
        }

        public RemoteMessage(bool success)
        {
            Success = success;
        }

        public RemoteMessage(bool success, string message, Exception exception)
        {
            Success = success;
            Message = message;
            Exception = exception;
        }

        public RemoteMessage(ReturnValue returnValue)
        {
            Success = returnValue.Success;
            Message = returnValue.Message;
            Exception = returnValue.Exception;
        }

        public RemoteMessage(string securityToken, string messageId, string method, KeyValuePair[] parameters, JToken value)
        {
            SecurityToken = securityToken;
            MessageId = messageId;
            Method = method;
            Success = true;
            Message = "";
            Exception = null;
            Value = value;
            Parameters = parameters;
        }

        public string MessageId { get; set; }
        public string SecurityToken { get; set; }
        public string Method { get; set; }
        public KeyValuePair[] Parameters { get; set; }
        public long HubKey { get; set; }
        public string RemoteAgentId { get; set; }
        public long? TimeOut { get; set; }

        public DexihHubVariable[] HubVariables { get; set; }

        public string GetParameter(string key)
        {
            if (Parameters == null)
            {
                return null;
            }

            foreach (var parameter in Parameters)
            {
                if (parameter.Key == key)
                {
                    return parameter.Value;
                }
            }

            return null;

        }

        public string GetParametersDetails()
        {
            if (Parameters == null)
            {
                return "";
            }

            var parameters = string.Join(",", Parameters.Select(c => c.Key + "=" + c.Value));
            return parameters;
        }
    }

    public class KeyValuePair
    {
        public KeyValuePair() { }

        public KeyValuePair(string key, string value)
        {
            Key = key;
            Value = value;
        }

        public string Key { get; set; }
        public string Value { get; set; }
    }
}

using Dexih.Utils.MessageHelpers;
using System;

namespace dexih.operations
{
    public sealed class Message : ReturnValue<object>
    {
        public string MessageId { get; set; }
        public string SecurityToken { get; set; }
        public string Command { get; set; }
        public long HubKey { get; set; }

        public Message() { }

        public Message(string securityToken, string messageId, string command, ReturnValue<object> returnValue)
        {
            SecurityToken = securityToken;
            MessageId = messageId;
            Command = command;
            Success = returnValue.Success;
            Message = returnValue.Message;
            Exception = returnValue.Exception;
            Value = returnValue.Value;
        }

        public Message(string securityToken, string messageId, string command, object value)
        {
            SecurityToken = securityToken;
            MessageId = messageId;
            Command = command;
            Success = true;
            Message = "";
            Exception = null;
            Value = value;
        }

        public Message(string securityToken, string messageId, string command, string returnValue)
        {
            SecurityToken = securityToken;
            MessageId = messageId;
            Command = command;
            Success = true;
            Message = "";
            Exception = null;
            Value = returnValue;
        }

        public Message(bool success, string message, Exception exception)
        {
            Success = success;
            Message = message;
            Exception = exception;
        }
    }
}

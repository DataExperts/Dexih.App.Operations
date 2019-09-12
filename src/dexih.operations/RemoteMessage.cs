using dexih.repository;
using Dexih.Utils.MessageHelpers;
using Newtonsoft.Json.Linq;
using ProtoBuf;
using System;
using System.Linq;

namespace dexih.operations
{
    [ProtoContract]
    public sealed class ResponseMessage : RemoteMessage
    {
        public ResponseMessage()
        {

        }
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

    [ProtoContract]
    [ProtoInclude(100, nameof(ResponseMessage))]
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

        public RemoteMessage(string securityToken, string messageId, string method, JToken value)
        {
            SecurityToken = securityToken;
            MessageId = messageId;
            Method = method;
            Success = true;
            Message = "";
            Exception = null;
            Value = value;
        }

        /// <summary>
        /// Unique id for the message
        /// </summary>
        [ProtoMember(1)]
        public string MessageId { get; set; }

        /// <summary>
        /// Secure token for the remote agent
        /// </summary>
        [ProtoMember(2)]
        public string SecurityToken { get; set; }

        /// <summary>
        /// Method being called
        /// </summary>
        [ProtoMember(3)]
        public string Method { get; set; }

        [ProtoMember(4)]
        public long HubKey { get; set; }

        [ProtoMember(5)]
        public long? TimeOut { get; set; }

        [ProtoMember(6)]
        public DexihHubVariable[] HubVariables { get; set; }

    }

}

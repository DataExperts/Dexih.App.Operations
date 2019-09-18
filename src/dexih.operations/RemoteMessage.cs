using dexih.repository;
using Dexih.Utils.MessageHelpers;
using Newtonsoft.Json.Linq;
using MessagePack;
using System;
using System.Linq;

namespace dexih.operations
{
 //   [MessagePackObject]
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

    [MessagePackObject]
    [MessagePack.Union(0, typeof(ResponseMessage))]
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
        [Key(0)]
        public string MessageId { get; set; }

        /// <summary>
        /// Secure token for the remote agent
        /// </summary>
        [Key(1)]
        public string SecurityToken { get; set; }

        /// <summary>
        /// Method being called
        /// </summary>
        [Key(2)]
        public string Method { get; set; }

        [Key(3)]
        public long HubKey { get; set; }

        [Key(4)]
        public long? TimeOut { get; set; }

        [Key(5)]
        public DexihHubVariable[] HubVariables { get; set; }

    }

}

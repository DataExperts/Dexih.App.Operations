using dexih.repository;
using Dexih.Utils.MessageHelpers;
using Newtonsoft.Json.Linq;
using MessagePack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace dexih.operations
{
    [DataContract]
    public sealed class ResponseMessage: ReturnValue<object>
    {
        public ResponseMessage()
        {

        }
        
        public override object Value { get; set; }
        
        /// <summary>
        /// Unique id for the message
        /// </summary>
        [DataMember(Order = 4)]
        public string MessageId { get; set; }

        /// <summary>
        /// Secure token for the remote agent
        /// </summary>
        [DataMember(Order = 5)]
        public string SecurityToken { get; set; }
        
        public ResponseMessage(string securityToken, string messageId, ReturnValue<object> returnMessage)
        {
            MessageId = messageId;
            SecurityToken = securityToken;
            Success = returnMessage.Success;
            Message = returnMessage.Message;
            Value = returnMessage.Value;
            Exception = returnMessage.Exception;
        }
    }

    [DataContract]
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

        public RemoteMessage(string securityToken, string messageId, JToken value)
        {
            SecurityToken = securityToken;
            MessageId = messageId;
            Success = true;
            Message = "";
            Exception = null;
            Value = value;
        }
 
        /// <summary>
        /// Unique id for the message
        /// </summary>
        [DataMember(Order = 4)]
        public string MessageId { get; set; }

        /// <summary>
        /// Secure token for the remote agent
        /// </summary>
        [DataMember(Order = 5)]
        public string SecurityToken { get; set; }

        /// <summary>
        /// Command 
        /// </summary>
        [DataMember(Order = 6)]
        public string Method { get; set; }

        
        [DataMember(Order = 7)]
        public long HubKey { get; set; }

        [DataMember(Order = 8)]
        public long? TimeOut { get; set; }

        [DataMember(Order = 9)]
        public DexihHubVariable[] HubVariables { get; set; }
        
        [DataMember(Order = 10)]
        public string ClientConnectionId { get; set; }

    }

  
}

//using System.Collections.Generic;
//using System.Runtime.Serialization;
//using Dexih.Utils.CopyProperties;
//using Dexih.Utils.Crypto;
//
//
//
//namespace dexih.operations
//{
//    [DataContract]
//    public class MessageValues: Dictionary<string, MessageValues>
//    {
//        [DataMember(Order = 0)]
//        public object Value { get; set; }
//        
//        public T ToObject<T>()
//        {
//            return (T) Value;
//        }
//
//        public MessageValues()
//        {
//            
//        }
//        
//        public MessageValues(object value)
//        {
//            Value = value;
//        }
//        
//        public static implicit operator MessageValues(string value)
//        {
//            return new MessageValues(value);
//        }
//
//        public static MessageValues FromObject(object value)
//        {
//            
//            return new MessageValues(value);
//        }
//        
//    }
//    
//
//}
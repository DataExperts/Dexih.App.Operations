using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace dexih.operations.Alerts
{
    public interface IAlertQueue: IDisposable
    {
        void Add(Alert message);
        
        Task WaitForMessage(CancellationToken cancellationToken);
        
        int Count { get; }
        
        bool TryDeque(out Alert result);

    }

    public class AlertQueue : IAlertQueue
    {
        private readonly BufferBlock<Alert> _messageQueue; 

        public AlertQueue()
        {
            _messageQueue = new BufferBlock<Alert>();
        }
        
        public void Add(Alert message)
        {
            _messageQueue.Post(message);
        }

        public int Count => _messageQueue.Count;

        public bool TryDeque(out Alert result)
        {
            return _messageQueue.TryReceive(out result);
        }
        
        public Task WaitForMessage(CancellationToken cancellationToken)
        {
            return _messageQueue.OutputAvailableAsync(cancellationToken);
        }
        
        public void Dispose()
        {
        }
    }
}
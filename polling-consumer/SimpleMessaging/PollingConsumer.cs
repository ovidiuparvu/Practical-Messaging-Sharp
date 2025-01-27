using System;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleMessaging
{
    public class PollingConsumer<T> where T: IAmAMessage
    {
        private readonly IAmAHandler<T> _messageHandler;
        private readonly Func<string, T> _messageSerializer;
        private readonly string _hostName;

        public PollingConsumer(IAmAHandler<T> messageHandler, Func<string, T> messageSerializer, string hostName = "localhost")
        {
            _messageHandler = messageHandler;
            _messageSerializer = messageSerializer;
            _hostName = hostName;
        }
        
        public Task Run(CancellationToken ct)
        {
            return Task.Run(async () =>
            {
                if (ct.IsCancellationRequested)
                {
                    return;
                }

                using var consumer = new DataTypeChannelConsumer<T>(_messageSerializer, _hostName);
                while (true)
                {
                    var message = consumer.Receive();
                    if (message != null)
                    {
                        _messageHandler.Handle(message);
                    }
                    else
                    {
                        Console.WriteLine("No message received");
                    }
                    await Task.Delay(1000, ct);
                    if (ct.IsCancellationRequested)
                    {
                        return;
                    }
                }
            }, ct);
        }
    }
}
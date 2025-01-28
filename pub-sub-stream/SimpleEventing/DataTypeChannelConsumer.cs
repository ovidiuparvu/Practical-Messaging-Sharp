using Confluent.Kafka;

namespace SimpleEventing;

public class DataTypeChannelConsumer<T> : IDisposable where T: IAmAMessage 
{
    private readonly Func<Message<string, string>, T> _translator;
    private readonly Func<T, bool> _handler;
    private readonly IConsumer<string,string> _consumer;

    public DataTypeChannelConsumer( Func<Message<string, string>, T> translator,  Func<T, bool> handler )
    {
        _translator = translator;
        _handler = handler;
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "SimpleEventing2",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            EnableAutoOffsetStore = false
        };

        _consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .SetErrorHandler((c, e) => Console.WriteLine(e.Reason))
            .SetLogHandler((c, m) => Console.WriteLine(m.Message))
            .SetPartitionsRevokedHandler((c, partitions) => partitions.ForEach(c.StoreOffset))
            .Build();

        var topic = "Pub-Sub-Stream-" + typeof(T).FullName;
        _consumer.Subscribe(topic);
    }
    
    public async Task Receive(CancellationToken cancellationToken)
    {
        try
        {
            while (true)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                var consumeResult = _consumer.Consume();
                if (consumeResult != null)
                {
                    if (consumeResult.IsPartitionEOF)
                    {
                        Console.WriteLine($"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");
                        continue;
                    }

                    var translatedMessage = _translator(consumeResult.Message);
                    if (_handler(translatedMessage))
                    {
                        _consumer.StoreOffset(consumeResult);
                    }
                    else
                    {
                        Console.WriteLine("Failed to handle message");
                    }
                }
                else
                {
                    Console.WriteLine("No message received");
                }
            }
        }
        catch(ConsumeException e)
        {
           Console.WriteLine(e.Message); 
        }
        catch (OperationCanceledException)
        {
            //Pump was cancelled, exit
        }
        finally
        {
            _consumer.Close();
        }
    }
    
    
    private void ReleaseUnmanagedResources()
    {
        _consumer.Close();
    }

    public void Dispose()
    {
        ReleaseUnmanagedResources();
        GC.SuppressFinalize(this);
    }

    ~DataTypeChannelConsumer()
    {
        ReleaseUnmanagedResources();
    }
}
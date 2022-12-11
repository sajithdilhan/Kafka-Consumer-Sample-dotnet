using Confluent.Kafka;

var config = new ConsumerConfig
{
    GroupId = "samplemessage-consumer",
    BootstrapServers = "localhost:9092",
    AutoOffsetReset = AutoOffsetReset.Latest
};

using (var consumer = new ConsumerBuilder<Null, string>(config).Build())
{
    consumer.Subscribe("kafkaproducer.samplemessage");

    try
    {
        while (true)
        {
            var response = consumer.Consume();
            var message = response.Message;
            if (response is not null && !string.IsNullOrEmpty(message.Value))
            {
                Console.WriteLine($"Message: {message.Value}");
                Console.WriteLine($"created at {TimeZoneInfo.ConvertTime(DateTimeOffset.FromUnixTimeMilliseconds(message.Timestamp.UnixTimestampMs), TimeZoneInfo.Local)}");
                Console.WriteLine($"received at {DateTime.Now}");
                Console.WriteLine("-------------------");
            }
        }
    }
    catch (KafkaException ex)
    {
        Console.WriteLine($"Kafka exception: {ex.Message}");
    }
    catch (Exception ex)
    {
        Console.WriteLine(ex.Message);
    }
}

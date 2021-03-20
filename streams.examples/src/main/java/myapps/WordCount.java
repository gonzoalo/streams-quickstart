package myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCount {
    public static void main(String[] args)  throws Exception{
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9002"); // assuming that the kafka brokr this application is talking to runs on local machine with port 9002
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream("streams-plaintext-input");

        source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
              .groupBy((key, value) -> value)
              .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
              .toStream()
              .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));


        // KStream<String, String> words = source.flatMapValues(new ValueMapper<String, Iterable<String>>() {
        //     @Override
        //     public Iterable<String> apply(String value) {
        //         return Arrays.asLlist(value.split("\\W+"));
        //     }
        // });

        // KStream<String, String> words = source.flatMapValues(value -> Arrays.asList(value.split("\\W+")));
        // source.flatMapValues(value -> Arrays.asList(value.split("\\W+"))).to("streams-linesplit-output");

        // using counts as a separable variable
        // KTable<String, Long> counts = source.flatMapValues(new ValueMapper<String, Iterable<String>>() {
        //     @Override
        //     public Iterable<String> apply(String value) {
        //         return Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"));
        //     }
        // })
        // .groupBy(new KeyValueMapper<String, String, String>()  {
        //     @Override
        //     public String apply(String key, String value) {
        //         return value;
        //     }
        // })
        // // Materialize the results into a KeyValueStore named "counts-store".
        // // The Materializd store is always of type <Byte, byte[]> as this is the format of the inner most store;
        // .count(Materialized.<String, Long, KeyValueStore<Byte, byte[]>> as("counts-store"));


        // counts.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        // with JDK 8 simplified form 

        // source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
        //     .groupBy((key, value) -> value)
        //     .count(Materialized.<String, Long, KeyValueStore<Byte, byte[]>>as("counts-store"))
        //     .toStream()
        //     .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        
        final Topology topology = builder.build();

        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
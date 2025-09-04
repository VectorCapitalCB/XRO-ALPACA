package cl.vc.xroalpaca.util;

import cl.vc.module.protocolbuff.generator.IDGenerator;
import lombok.Data;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@lombok.extern.slf4j.Slf4j
@Data
public class KafkaAdapter extends Thread {

    private Properties properties;

    private KafkaProducer<String, String> producer;

    public KafkaAdapter(Properties properties) {

        try {

            String groupId = properties.getProperty("app.stream.kafka.binder.group");

            this.properties = new Properties();
            this.properties.put("bootstrap.servers", properties.getProperty("app.stream.kafka.binder.brokers"));
            this.properties.put("enable.auto.commit", "false");
            this.properties.put("auto.commit.interval.ms", "1000");
            this.properties.put("session.timeout.ms", "30000");
            this.properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            this.properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            this.properties.put("auto.offset.reset", properties.getProperty("app.stream.kafka.binder.offset.reset") != null
                    ? properties.getProperty("app.stream.kafka.binder.offset.reset") : "latest");

            if ("earliest".equalsIgnoreCase(this.properties.get("auto.offset.reset").toString())) {
                groupId += "-" + IDGenerator.getID();
            }

            this.properties.put("group.id", groupId);

            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("app.stream.kafka.binder.brokers"));
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producer = new KafkaProducer<>(producerProps);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}

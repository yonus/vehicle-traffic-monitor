package com.vehicletrafficmonitor.generator.producers.kafka;

import com.vehicletrafficmonitor.generator.model.VehicleLocationData;
import com.vehicletrafficmonitor.generator.producers.VehicleLocationDataProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.function.Supplier;

public class KafkaVehicleLocationDataProducer implements VehicleLocationDataProducer {

    private  Properties kafkaFileProperties = new Properties();
    private static final Logger logger = Logger.getLogger(KafkaVehicleLocationDataProducer.class);
    private static final String PROPERTY_FILE_PATH = "kafka-producer.properties";

    private Producer<String,VehicleLocationData> producer;

    public KafkaVehicleLocationDataProducer() {
        try {
            readKafkaProducerProperties();
            Properties kafkaProducerProperties = filePropertiesToProducerProperties(this.kafkaFileProperties);
            producer = new KafkaProducer<>(kafkaProducerProperties);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void produce(VehicleLocationData vehicleLocationData) {
        producer.send(new ProducerRecord<>(getTopicName(),vehicleLocationData));

    }

    @Override
    public Runnable getShutDownHookSupplier() {
        return () -> {
            if (producer != null) {
                producer.flush();
                producer.close();
            }
        };

    }


    private Properties filePropertiesToProducerProperties(Properties kafkaFileProperties){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaFileProperties.getProperty("com.vehicletrafficmonitor.kafka.brokerlist"));
        properties.put("acks", "all");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.vehicletrafficmonitor.generator.producers.kafka.serializer.VehicleLocationDataKafkaSerializer");
        return properties;
    }


    private void readKafkaProducerProperties() throws IOException {
        if (kafkaFileProperties.isEmpty()) {

            try(InputStream input = KafkaVehicleLocationDataProducer.class.getClassLoader().getResourceAsStream(PROPERTY_FILE_PATH)) {
                kafkaFileProperties.load(input);
            } catch (IOException ex) {
                logger.error(ex);
                throw ex;
            }

        }
    }

    private String getTopicName(){
        if(!kafkaFileProperties.isEmpty()){
            return  kafkaFileProperties.getProperty("com.vehicletrafficmonitor.kafka.topic");
        }
        return null;
    }


}

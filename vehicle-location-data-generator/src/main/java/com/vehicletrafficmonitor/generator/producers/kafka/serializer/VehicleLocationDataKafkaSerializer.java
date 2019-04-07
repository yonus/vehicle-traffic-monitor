package com.vehicletrafficmonitor.generator.producers.kafka.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vehicletrafficmonitor.generator.model.VehicleLocationData;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.Logger;

import java.util.Map;


public class VehicleLocationDataKafkaSerializer implements Serializer<VehicleLocationData> {

    private static final Logger logger = Logger.getLogger(VehicleLocationDataKafkaSerializer.class);
    private static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, VehicleLocationData vehicleLocationData) {
        try {
            String msg = objectMapper.writeValueAsString(vehicleLocationData);
            logger.info(msg);
            return msg.getBytes();
        } catch (JsonProcessingException e) {
            logger.error("Error in Serialization", e);
            throw  new RuntimeException("Error in Serialization",e);
        }
    }

    @Override
    public void close() {

    }
}

package com.vehicletrafficmonitor.generator.producers;

import com.vehicletrafficmonitor.generator.producers.kafka.KafkaVehicleLocationDataProducer;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class VehicleLocationDataProducerFactory {

    public static enum ProducerClient {
        KAFKA;
    }

    private static final Map<ProducerClient, Supplier<VehicleLocationDataProducer>>  producerClients = new HashMap<>();


    static{
            producerClients.put(ProducerClient.KAFKA, KafkaVehicleLocationDataProducer::new);
    }


    public static VehicleLocationDataProducer getVehicleLocationDataProducer(ProducerClient producerClient){
         Supplier<VehicleLocationDataProducer> vehicleLocationDataProducerSupplier =  producerClients.get(producerClient);
         return vehicleLocationDataProducerSupplier.get();
    }

}

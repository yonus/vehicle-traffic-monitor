package com.vehicletrafficmonitor.generator.producers;

import com.vehicletrafficmonitor.generator.model.VehicleLocationData;



public interface VehicleLocationDataProducer {

   void produce(VehicleLocationData vehicleLocationData);

   Runnable getShutDownHookSupplier();
}

package com.vehicletrafficmonitor.generator;

import com.vehicletrafficmonitor.generator.model.VehicleLocationData;
import com.vehicletrafficmonitor.generator.producers.VehicleLocationDataProducer;
import com.vehicletrafficmonitor.generator.producers.VehicleLocationDataProducerFactory;
import org.apache.log4j.Logger;

import java.util.*;

public class VehicleLocationDataGenerator {
    private static final Logger logger = Logger.getLogger(VehicleLocationDataGenerator.class);
    private static Random random = new Random();


    public static void main(String[] args){

        VehicleLocationDataProducer vehicleLocationDataProducer = VehicleLocationDataProducerFactory.getVehicleLocationDataProducer(VehicleLocationDataProducerFactory.ProducerClient.KAFKA);
        addShutDownHook(vehicleLocationDataProducer.getShutDownHookSupplier());
        try {
            generateData(vehicleLocationDataProducer);
        } catch (InterruptedException e) {
            logger.error("Error in main",e);
        }

    }



    public static  void addShutDownHook(Runnable runnable){
        Runtime.getRuntime().addShutdownHook(new Thread(runnable));
    }



    public static void generateData(VehicleLocationDataProducer vehicleLocationDataProducer) throws InterruptedException {
        List<String> routeList = Arrays.asList(new String[]{"Route-37", "Route-43", "Route-82"});
        List<String> vehicleTypeList = Arrays.asList(new String[]{"Large Truck", "Small Truck", "Private Car", "Bus", "Taxi"});

        logger.info("Sending events");

            // generate event in loop
            while (true) {
                List<VehicleLocationData> eventList = new ArrayList<>();
                for (int i = 0; i < 100; i++) {// create 100 vehicles
                    String vehicleId = UUID.randomUUID().toString();
                    String vehicleType = vehicleTypeList.get(random.nextInt(5));
                    String routeId = routeList.get(random.nextInt(3));
                    Date timestamp = new Date();
                    double speed = random.nextInt(100 - 20) + 20;// random speed between 20 to 100
                    double fuelLevel = random.nextInt(40 - 10) + 10;
                    for (int j = 0; j < 5; j++) {// Add 5 events for each vehicle
                        String coords = getCoordinates(routeId);
                        String latitude = coords.substring(0, coords.indexOf(","));
                        String longitude = coords.substring(coords.indexOf(",") + 1, coords.length());
                        VehicleLocationData event = new VehicleLocationData(vehicleId, vehicleType, routeId, latitude, longitude, timestamp, speed,fuelLevel);
                        eventList.add(event);
                    }
                }
                Collections.shuffle(eventList);// shuffle for random events
                for (VehicleLocationData event : eventList) {
                    vehicleLocationDataProducer.produce(event);
                    Thread.sleep(random.nextInt(3000 - 1000) + 1000);//random delay of 1 to 3 seconds
                }
            }
        }

    //Method to generate random latitude and longitude for routes
    private static String  getCoordinates(String routeId) {

        int latPrefix = 0;
        int longPrefix = -0;
        if (routeId.equals("Route-37")) {
            latPrefix = 33;
            longPrefix = -96;
        } else if (routeId.equals("Route-82")) {
            latPrefix = 34;
            longPrefix = -97;
        } else if (routeId.equals("Route-43")) {
            latPrefix = 35;
            longPrefix = -98;
        }
        Float lati = latPrefix + random.nextFloat();
        Float longi = longPrefix + random.nextFloat();
        return lati + "," + longi;
    }

}

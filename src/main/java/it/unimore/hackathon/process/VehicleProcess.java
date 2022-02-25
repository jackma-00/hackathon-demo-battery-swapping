package it.unimore.hackathon.process;

import it.unimore.hackathon.vehicles.VehicleMqttObject;
import it.unimore.hackathon.resource.BatterySensorResource;
import it.unimore.hackathon.resource.GpsGpxSensorResource;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.UUID;

public class VehicleProcess {

    private static final Logger logger = LoggerFactory.getLogger(VehicleProcess.class);

    private static String MQTT_BROKER_IP = "127.0.0.1";

    private static int MQTT_BROKER_PORT = 1883;

    public static void main(String[] args) {

        try{

            //Generate Random Vehicle UUID
            String vehicleId = UUID.randomUUID().toString();

            //Create MQTT Client
            MqttClientPersistence persistence = new MemoryPersistence();
            IMqttClient mqttClient = new MqttClient(String.format("tcp://%s:%d",
                    MQTT_BROKER_IP,
                    MQTT_BROKER_PORT),
                    vehicleId,
                    persistence);

            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(10);

            //Connect to MQTT Broker
            mqttClient.connect(options);

            logger.info("MQTT Client Connected ! Client Id: {}", vehicleId);

            VehicleMqttObject vehicleMqttObject = new VehicleMqttObject();
            vehicleMqttObject.init(vehicleId, mqttClient, new HashMap<>(){
                {
                    put("gps", new GpsGpxSensorResource());
                    put("battery", new BatterySensorResource());
                }
            });

            vehicleMqttObject.start();

        }catch (Exception e){
            e.printStackTrace();
        }

    }

}

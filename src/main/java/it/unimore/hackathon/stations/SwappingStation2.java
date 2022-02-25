package it.unimore.hackathon.stations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jenetics.jpx.Length;
import io.jenetics.jpx.Point;
import io.jenetics.jpx.WayPoint;
import io.jenetics.jpx.geom.Geoid;
import it.unimore.hackathon.message.ControlMessage;
import it.unimore.hackathon.message.TelemetryMessage;
import it.unimore.hackathon.model.GpsLocationDescriptor;
import it.unimore.hackathon.resource.BatterySensorResource;
import it.unimore.hackathon.resource.GpsGpxSensorResource;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;

public class SwappingStation2 {

    private final static Logger logger = LoggerFactory.getLogger(SwappingStation2.class);

    private final static String stationID = UUID.randomUUID().toString();

    private final static double stationLatitude = 45.183205605;

    private final static double stationLongitude = 10.769798756;

    private final static double actionRange = 9000;

    private static final double ALARM_BATTERY_LEVEL = 20.0;

    private static final String CONTROL_TOPIC = "control";

    //IP Address of the target MQTT Broker
    private static String BROKER_ADDRESS = "127.0.0.1";

    //PORT of the target MQTT Broker
    private static int BROKER_PORT = 1883;

    //E.g. fleet/vehicle/e0c7433d-8457-4a6b-8084-595d500076cc/telemetry/gps
    private static final String GPS_TOPIC = "fleet/vehicle/+/telemetry/gps";

    //E.g. fleet/vehicle/e0c7433d-8457-4a6b-8084-595d500076cc/telemetry/battery
    private static final String BATTERY_TOPIC = "fleet/vehicle/+/telemetry/battery";

    private static final String ALARM_MESSAGE_CONTROL_TYPE = "battery_alarm_message";

    private static ObjectMapper mapper;

    private static boolean proximity = false; // flag to state the proximity of the vehicle with the station


    public static void main(String [ ] args) {

    	logger.info("MQTT Consumer Tester Started ...");

        try{

            //Generate a random MQTT client ID using the UUID class
            String clientId = UUID.randomUUID().toString();

            //Represents a persistent data store
            MqttClientPersistence persistence = new MemoryPersistence();

            //Create MQTT client
            IMqttClient client = new MqttClient(
                    String.format("tcp://%s:%d", BROKER_ADDRESS, BROKER_PORT), //Create the URL from IP and PORT
                    clientId,
                    persistence);

            //Define MQTT Connection Options
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(10);

            //Connect to the target broker
            client.connect(options);

            logger.info("Connected ! Client Id: {}", clientId);

            mapper = new ObjectMapper();

            // SUBSCRIPTIONS -------------------------------------------------------------------------------------------

            //Subscribe to GPS topic and compute the proximity
            logger.info("Subscribing to topic: {}", GPS_TOPIC);

            client.subscribe(GPS_TOPIC, (topic, msg) -> {

                Optional<TelemetryMessage<GpsLocationDescriptor>> telemetryMessageOptional = parseGpsMessagePayload(msg);

                if(telemetryMessageOptional.isPresent() && telemetryMessageOptional.get().getType().equals(GpsGpxSensorResource.RESOURCE_TYPE)){

                    GpsLocationDescriptor gpsLocationDescriptor = telemetryMessageOptional.get().getDataValue();

                    logger.info("Lat: {} - Long: {}", gpsLocationDescriptor.getLatitude(),
                            gpsLocationDescriptor.getLongitude());

                    // computing distance
                    final Point start = WayPoint.of(stationLatitude, stationLongitude);
                    final Point end = WayPoint.of(gpsLocationDescriptor.getLatitude(), gpsLocationDescriptor.getLongitude());
                    final Length distance = Geoid.WGS84.distance(start, end);
                    logger.info("Distance: {}", distance);

                    if (distance.doubleValue() < actionRange) {
                        proximity = true;
                    } else {
                        proximity = false;
                    }

                }
            });

            //Subscribe to BATTERY topic and evaluate the charge level
            logger.info("Subscribing to topic: {}", BATTERY_TOPIC);

            client.subscribe(BATTERY_TOPIC, (topic, msg) -> {

                Optional<TelemetryMessage<Double>> telemetryMessageOptional = parseBatteryMessagePayload(msg);

                if(telemetryMessageOptional.isPresent() && telemetryMessageOptional.get().getType().equals(BatterySensorResource.RESOURCE_TYPE)) {

                    Double newBatteryLevel = telemetryMessageOptional.get().getDataValue();
                    logger.info("New Battery Telemetry Data Received ! Battery Level: {}", newBatteryLevel);

                    if (newBatteryLevel < ALARM_BATTERY_LEVEL && proximity) {

                        logger.info("BATTERY LEVEL ALARM DETECTED ! Sending Control Notification ...");

                        String controlTopic = String.format("%s/%s", topic.replace("/telemetry/battery", ""), CONTROL_TOPIC);
                        publishControlMessage(client, controlTopic, new ControlMessage(ALARM_MESSAGE_CONTROL_TYPE, new HashMap<>(){
                            {
                                put("swapping_station_id", stationID);
                                put("swapping_station_latitude", stationLatitude);
                                put("swapping_station_longitude", stationLongitude);
                            }
                        }));

                    }
                }

            });

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static Optional<TelemetryMessage<GpsLocationDescriptor>> parseGpsMessagePayload(MqttMessage mqttMessage){

        try{

            if(mqttMessage == null)
                return Optional.empty();

            byte[] payloadByteArray = mqttMessage.getPayload();
            String payloadString = new String(payloadByteArray);

            return Optional.ofNullable(mapper.readValue(payloadString, new TypeReference<TelemetryMessage<GpsLocationDescriptor>>() {}));

        }catch (Exception e){
            e.printStackTrace();
            return Optional.empty();
        }
    }

    private static boolean isBatteryLevelAlarm(Double originalValue, Double newValue){
        return originalValue - newValue >= ALARM_BATTERY_LEVEL;
    }

    private static Optional<TelemetryMessage<Double>> parseBatteryMessagePayload(MqttMessage mqttMessage){

        try{

            if(mqttMessage == null)
                return Optional.empty();

            byte[] payloadByteArray = mqttMessage.getPayload();
            String payloadString = new String(payloadByteArray);

            return Optional.ofNullable(mapper.readValue(payloadString, new TypeReference<TelemetryMessage<Double>>() {}));

        }catch (Exception e){
            return Optional.empty();
        }
    }

    private static void publishControlMessage(IMqttClient mqttClient, String topic, ControlMessage controlMessage) throws MqttException, JsonProcessingException {

        new Thread(new Runnable() {
            @Override
            public void run() {

                try{

                    logger.info("Sending to topic: {} -> Data: {}", topic, controlMessage);

                    if(mqttClient != null && mqttClient.isConnected() && controlMessage != null && topic != null){

                        String messagePayload = mapper.writeValueAsString(controlMessage);

                        MqttMessage mqttMessage = new MqttMessage(messagePayload.getBytes());
                        mqttMessage.setQos(0);

                        mqttClient.publish(topic, mqttMessage);

                        logger.info("Data Correctly Published to topic: {}", topic);

                    }
                    else
                        logger.error("Error: Topic or Msg = Null or MQTT Client is not Connected !");

                }catch (Exception e){
                    e.printStackTrace();
                }

            }
        }).start();
    }
}

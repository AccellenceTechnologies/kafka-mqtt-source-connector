package com.sintef.asam;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MqttSourceConnectorTask extends SourceTask implements MqttCallback {

    private MqttClient mqttClient;
    private String kafkaTopic;
    private String mqttTopic;
    private String mqttClientId;
    private String connectorName;
    private MqttSourceConnectorConfig connectorConfiguration;
    BlockingQueue<SourceRecord> mqttRecordQueue = new LinkedBlockingQueue<>();
    private static final Logger logger = LogManager.getLogger(MqttSourceConnectorTask.class);

    private void initMqttClient() {
        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        mqttConnectOptions.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
        mqttConnectOptions
                .setServerURIs(new String[] { connectorConfiguration.getString("mqtt.connector.broker.uri") });
        mqttConnectOptions.setConnectionTimeout(connectorConfiguration.getInt("mqtt.connector.connection_timeout"));
        mqttConnectOptions.setKeepAliveInterval(connectorConfiguration.getInt("mqtt.connector.keep_alive"));
        mqttConnectOptions.setCleanSession(connectorConfiguration.getBoolean("mqtt.connector.clean_session"));
        mqttConnectOptions.setUserName(connectorConfiguration.getString("mqtt.connector.username"));
        mqttConnectOptions
                .setPassword(connectorConfiguration.getPassword("mqtt.connector.password").value().toCharArray());
        mqttConnectOptions.setAutomaticReconnect(true); // Automatisches Wiederverbinden aktivieren

        try {
            mqttClientId = MqttAsyncClient.generateClientId(); // Einzigartige Client-ID generieren
            mqttClient = new MqttClient(connectorConfiguration.getString("mqtt.connector.broker.uri"), mqttClientId,
                    new MemoryPersistence());
            mqttClient.setCallback(this);
            mqttClient.connect(mqttConnectOptions);
            logger.info("SUCCESSFUL MQTT CONNECTION for AsamMqttSourceConnectorTask: '{}', and mqtt client: '{}'.",
                    connectorName, mqttClientId);
        } catch (MqttException e) {
            logger.error("FAILED MQTT CONNECTION for AsamMqttSourceConnectorTask: '{}', and mqtt client: '{}'.",
                    connectorName, mqttClientId);
            logger.error(e);
        }

        try {
            mqttClient.subscribe(mqttTopic, connectorConfiguration.getInt("mqtt.connector.qos"));
            logger.info("SUCCESSFUL MQTT SUBSCRIPTION for MqttSourceConnectorTask: '{}', and mqtt client: '{}'.",
                    connectorName, mqttClientId);
        } catch (MqttException e) {
            logger.error("FAILED MQTT SUBSCRIPTION for MqttSourceConnectorTask: '{}', and mqtt client: '{}'.",
                    connectorName, mqttClientId);
            e.printStackTrace();
        }
    }

    @Override
    public String version() {
        return "1.0"; // Standardversion
    }

    @Override
    public void start(Map<String, String> map) {
        connectorConfiguration = new MqttSourceConnectorConfig(map);
        connectorName = connectorConfiguration.getString("mqtt.connector.kafka.name");
        kafkaTopic = connectorConfiguration.getString("mqtt.connector.kafka.topic");
        mqttTopic = connectorConfiguration.getString("mqtt.connector.broker.topic");
        logger.info("Starting AsamMqttSourceConnectorTask with connector name: '{}'", connectorName);
        initMqttClient();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        records.add(mqttRecordQueue.take());
        return records;
    }

    @Override
    public void stop() {
        try {
            if (mqttClient != null && mqttClient.isConnected()) {
                mqttClient.disconnect();
                mqttClient.close(); // Sicherstellen, dass der Client geschlossen wird
                logger.info("Disconnected and closed MQTT client for connector: '{}'.", connectorName);
            }
        } catch (MqttException e) {
            logger.error("Error during MQTT disconnection for connector: '{}'.", connectorName, e);
        }
    }

    @Override
    public void connectionLost(Throwable throwable) {
        logger.error("Connection for connector: '{}', running client: '{}', lost to topic: '{}'.", connectorName,
                mqttClientId, mqttTopic);
        if (throwable != null) {
            logger.error("Reason for disconnection: {}", throwable.getMessage());
            logger.error("Stack trace:", throwable);
        }
    }

    @Override
    public void messageArrived(String tempMqttTopic, MqttMessage mqttMessage) {
        logger.debug("Mqtt message arrived to connector: '{}', running client: '{}', on topic: '{}'.", connectorName,
                mqttClientId, tempMqttTopic);
        try {
            byte[] payload = mqttMessage.getPayload(); // Payload als Byte-Array
            logger.debug("Mqtt message payload: '{}'", new String(payload)); // Optional: Als String für Debugging
            mqttRecordQueue.put(new SourceRecord(Collections.singletonMap("mqttTopic", tempMqttTopic),
                    Collections.singletonMap("offset", System.currentTimeMillis()), kafkaTopic, null,
                    Schema.BYTES_SCHEMA, payload));
        } catch (Exception e) {
            logger.error(
                    "ERROR: Not able to create source record from mqtt message '{}' arrived on topic '{}' for client '{}'.",
                    mqttMessage.toString(), tempMqttTopic, mqttClientId);
            logger.error(e);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        // Optional: Handle message delivery completion if needed
    }
}

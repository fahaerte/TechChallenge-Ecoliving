#include "Secrets.h"
#include <WiFiClientSecure.h>
#include <MQTT.h>
#include <SensirionI2CScd4x.h>
#include <ArduinoJson.h>
#include "WiFi.h"
#include <Wire.h>


SensirionI2CScd4x scd4x;

// The MQTT topics that this device should publish/subscribe
#define AWS_IOT_TOPIC "$aws/things/" THINGNAME "/shadow/name/update"

WiFiClientSecure net = WiFiClientSecure();
MQTTClient client = MQTTClient(512);

void connectAWS()
{
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

  Serial.println("Connecting to Wi-Fi");

  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }

  Serial.println("Connected to Wi-Fi");

  // Configure WiFiClientSecure to use the AWS IoT device credentials
  net.setCACert(AWS_CERT_CA);
  net.setCertificate(AWS_CERT_CRT);
  net.setPrivateKey(AWS_CERT_PRIVATE);

  // Connect to the MQTT broker on the AWS endpoint we defined earlier
  client.begin(AWS_IOT_ENDPOINT, 8883, net);

  Serial.print("Connecting to AWS IOT");

  while (!client.connect(THINGNAME)) {
    Serial.print(".");
    delay(100);
  }

  if (!client.connected()) {
    Serial.println("AWS IoT Timeout!");
    return;
  }

  Serial.println("AWS IoT Connected!");
}

void publishMessage(uint16_t co2, float temperature, float humidity)
{
  StaticJsonDocument<512> doc;

  // Write temperature / humidity / other sensor data
  doc["temperature"] = temperature;
  doc["humidity"] = humidity;
  doc["CO2_level"] = co2;
  doc["device_id"] = 1;

  Serial.println(co2);
  Serial.println(temperature);
  Serial.println(humidity);

  char jsonBuffer[512];
  serializeJson(doc, jsonBuffer); // print to client
  client.publish(AWS_IOT_TOPIC, jsonBuffer);
}


void setup() {
  Serial.begin(9600);

  uint16_t error;
  char errorMessage[256];
  
  Wire.begin();
  scd4x.begin(Wire);


  uint16_t serial0;
  uint16_t serial1;
  uint16_t serial2;
  error = scd4x.getSerialNumber(serial0, serial1, serial2);
  if (error) {
    Serial.print("Error trying to execute getSerialNumber(): ");
    errorToString(error, errorMessage, 256);
    Serial.println(errorMessage);
    } 

    // Start Measurement
    error = scd4x.startPeriodicMeasurement();
    if (error) {
      Serial.print("Error trying to execute startPeriodicMeasurement(): ");
      errorToString(error, errorMessage, 256);
      Serial.println(errorMessage);
    }
    
  connectAWS();
}

void loop() {
  uint16_t error;
  char errorMessage[256];

  uint16_t co2 = 0;
  float temperature = 0.0f;
  float humidity = 0.0f;
  error = scd4x.readMeasurement(co2, temperature, humidity);

  if (error) {
    Serial.print("Error trying to execute readMeasurement(): ");
    errorToString(error, errorMessage, 256);
    Serial.println(errorMessage);
    } else if (co2 == 0) {
      Serial.println("Invalid sample detected, skipping.");
    } else {
      publishMessage(co2, temperature, humidity);
      // client.loop();
      delay(1000);
    }
  
  
}

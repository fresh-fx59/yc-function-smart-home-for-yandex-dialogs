from my_logger import logger
import paho.mqtt.client as mqtt_client
import time
from mqtt_config import MQTTConfig


class MQTTClient:
    def __init__(self, client_id=None):
        # Initialize client for paho-mqtt 2.0+
        self._client = mqtt_client.Client(
            client_id=client_id,
            protocol=mqtt_client.MQTTv311,
            callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2
        )

        self.message_received = False
        self.received_payload = None

        # Bind generic class methods to paho callbacks
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, rc):
        """Internal callback when a connection is established."""
        if rc == 0:
            logger.info("✅ Connection successful.")
        else:
            logger.error(f"❌ Connection failed with result code {rc}")

    def _on_message(self, client, userdata, msg):
        """Internal callback when a message is received."""
        logger.info(f"📥 Message received on topic {msg.topic}")
        try:
            self.received_payload = msg.payload.decode('utf-8')
        except UnicodeDecodeError:
            # Handle binary payloads
            self.received_payload = msg.payload
        self.message_received = True

    def connect_certs(self):
        """Set up TLS with client certificates and connect."""
        try:
            # TLS Setup with CA, client certificate, and key
            self._client.tls_set(
                ca_certs=MQTTConfig.CERTIFICATE_PATH,
                certfile=MQTTConfig.CLIENT_CERT_PATH,
                keyfile=MQTTConfig.CLIENT_KEY_PATH,
                tls_version=MQTTConfig.TLS_VERSION
            )
            # Disable certificate verification for self-signed certificates if needed
            self._client.tls_insecure_set(False)
        except FileNotFoundError as e:
            logger.error(f"Fatal Error: Certificate file not found: {e}")
            raise
        except Exception as e:
            logger.error(f"Fatal Error: TLS setup failed: {e}")
            raise

        self._connect_internal()

    def connect_auth(self, username: str, password: str):
        """Set up authentication with username/password (JWT) and connect."""
        self._client.username_pw_set(username=username, password=password)

        try:
            # TLS Setup with ONLY CA certs
            self._client.tls_set(
                ca_certs=MQTTConfig.CERTIFICATE_PATH,
                tls_version=MQTTConfig.TLS_VERSION
            )
            # Disable certificate verification for self-signed certificates if needed
            self._client.tls_insecure_set(False)
        except FileNotFoundError as e:
            logger.error(f"Fatal Error: CA certificate file not found: {e}")
            raise
        except Exception as e:
            logger.error(f"Fatal Error: TLS setup failed: {e}")
            raise

        self._connect_internal()

    def _connect_internal(self):
        """Internal method to perform the actual connection."""
        try:
            result = self._client.connect(MQTTConfig.MQTT_HOST, MQTTConfig.MQTT_PORT,
                                          MQTTConfig.CONNECT_TIMEOUT_SECONDS)
            if result != mqtt_client.MQTT_ERR_SUCCESS:
                raise Exception(f"Connection failed with code {result}")
        except Exception as e:
            logger.error(f"Fatal Error: Could not connect to MQTT broker: {e}")
            raise

    def subscribe_and_wait(self, mqtt_topic: str):
        """Subscribes to a topic and waits for a single message."""
        try:
            # 1. Subscribe after connection
            result, mid = self._client.subscribe(mqtt_topic)
            if result != mqtt_client.MQTT_ERR_SUCCESS:
                logger.error(f"Subscription failed with code {result}")
                return None

            # 2. Start the network loop
            self._client.loop_start()
            time.sleep(MQTTConfig.LOOP_SETUP_DELAY)  # Give time for subscription ACK

            logger.info(f"⏳ Waiting for a message on '{mqtt_topic}' for {MQTTConfig.WAIT_TIME_SECONDS}s...")

            # 3. Wait loop
            start_time = time.time()
            while time.time() - start_time < MQTTConfig.WAIT_TIME_SECONDS:
                if self.message_received:
                    logger.info(f"Successfully retrieved payload from {mqtt_topic}.")
                    break
                time.sleep(0.1)

            # 4. Cleanup
            self._client.loop_stop()
            self._client.disconnect()

            # 5. Return result
            if self.message_received:
                return self.received_payload
            else:
                logger.info(f"Timeout reached. 💔 No message received on '{mqtt_topic}'.")
                return None

        except Exception as e:
            logger.error(f"Error during subscribe and wait: {e}")
            # Ensure cleanup even if an error occurs
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except:
                pass
            return None
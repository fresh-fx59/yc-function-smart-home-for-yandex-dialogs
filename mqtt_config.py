import ssl


class MQTTConfig:
    MQTT_HOST = "mqtt.cloud.yandex.net"
    MQTT_PORT = 8883

    # Path constants
    CERTIFICATE_PATH = "certificate.pem"
    CLIENT_CERT_PATH = "client-cert.pem"
    CLIENT_KEY_PATH = "client-key.pem"

    # Time constants
    WAIT_TIME_SECONDS = 10
    CONNECT_TIMEOUT_SECONDS = 60
    LOOP_SETUP_DELAY = 0.5  # Delay after loop_start for subscription handshake
    TLS_VERSION = ssl.PROTOCOL_TLSv1_2  # More flexible than TLS v1.2 specifically
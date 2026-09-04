"""Constants for the Hisense TV integration."""

ATTR_CODE = "auth_code"

CONF_MQTT_PORT = "mqtt_port"
CONF_TLS_CA_CERT = "tls_ca_cert"
CONF_TLS_CLIENT_CERT = "tls_client_cert"
CONF_TLS_CLIENT_KEY = "tls_client_key"

DATA_CLIENT = "client"
DATA_KEY = "media_player.hisense_tv"

DEFAULT_CLIENT_ID = "HomeAssistant"
DEFAULT_MQTT_PORT = 36669
# These are the well-known, publicly documented default credentials for the
# on-device broker used across Hisense/VIDAA TVs - not a secret specific to
# any one TV. The CA/client certificate files still have to be supplied by
# the user - the same files already used for the Mosquitto bridge.
DEFAULT_MQTT_USERNAME = "hisenseservice"
DEFAULT_MQTT_PASSWORD = "multimqttservice"
DEFAULT_NAME = "Hisense TV"

DOMAIN = "hisense_tv"

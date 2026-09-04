"""Component init"""
import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_IP_ADDRESS
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady

from .const import (
    CONF_MQTT_PORT,
    CONF_TLS_CA_CERT,
    CONF_TLS_CLIENT_CERT,
    CONF_TLS_CLIENT_KEY,
    DATA_CLIENT,
    DEFAULT_CLIENT_ID,
    DEFAULT_MQTT_PASSWORD,
    DEFAULT_MQTT_PORT,
    DEFAULT_MQTT_USERNAME,
    DOMAIN,
)
from .mqtt_client import HisenseMqttClient, HisenseMqttError

_LOGGER = logging.getLogger(__name__)

PLATFORMS = ["media_player", "switch", "sensor"]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry):
    """Set up HisenseTV from a config entry."""
    _LOGGER.debug("async_setup_entry")

    client = HisenseMqttClient(
        hass=hass,
        host=entry.data[CONF_IP_ADDRESS],
        port=entry.data.get(CONF_MQTT_PORT, DEFAULT_MQTT_PORT),
        username=DEFAULT_MQTT_USERNAME,
        password=DEFAULT_MQTT_PASSWORD,
        client_id=DEFAULT_CLIENT_ID,
        ca_cert=entry.data.get(CONF_TLS_CA_CERT),
        client_cert=entry.data.get(CONF_TLS_CLIENT_CERT),
        client_key=entry.data.get(CONF_TLS_CLIENT_KEY),
    )
    try:
        await client.async_connect()
    except HisenseMqttError as err:
        raise ConfigEntryNotReady(
            f"Could not connect to the TV's broker at "
            f"{entry.data[CONF_IP_ADDRESS]}: {err}"
        ) from err

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = {DATA_CLIENT: client}

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass, entry):
    """Unload HisenseTV config entry."""
    _LOGGER.debug("async_unload_entry")
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        data = hass.data[DOMAIN].pop(entry.entry_id)
        await data[DATA_CLIENT].async_disconnect()
    return unload_ok


async def async_setup(hass, config):
    """Set up the HisenseTV integration."""
    _LOGGER.debug("async_setup")
    hass.data.setdefault(DOMAIN, {})
    return True

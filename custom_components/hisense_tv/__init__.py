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


async def _async_request_state(client: HisenseMqttClient) -> None:
    """Ask the TV to (re)send everything the entities display.

    The answers arrive on topics the entities already subscribe to. Without
    this, data only showed up when the TV happened to broadcast something or
    when the Picture Settings sensor next polled (up to a minute and a half).
    """
    cid = DEFAULT_CLIENT_ID
    await client.async_publish(f"/remoteapp/tv/ui_service/{cid}/actions/gettvstate", "")
    await client.async_publish(
        f"/remoteapp/tv/platform_service/{cid}/actions/getvolume", ""
    )
    await client.async_publish(f"/remoteapp/tv/ui_service/{cid}/actions/sourcelist", "")
    await client.async_publish(
        f"/remoteapp/tv/platform_service/{cid}/actions/picturesetting",
        '{"action": "get_menu_info"}',
    )


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
        # required=False: if the TV is off right now, don't fail the setup (HA
        # would retry with growing pauses). The client keeps trying in the
        # background and connects as soon as the TV is reachable.
        await client.async_connect(timeout=3, required=False)
    except HisenseMqttError as err:
        raise ConfigEntryNotReady(
            f"Could not connect to the TV's broker at "
            f"{entry.data[CONF_IP_ADDRESS]}: {err}"
        ) from err

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = {DATA_CLIENT: client}

    try:
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    except Exception:
        # Don't leave a half-set-up client running - two clients with the same
        # client id would keep kicking each other off the broker.
        hass.data[DOMAIN].pop(entry.entry_id, None)
        await client.async_disconnect()
        raise

    async def _refresh() -> None:
        await _async_request_state(client)

    # Ask for the current state on every (re)connect - this is what makes data
    # appear right after the TV is switched on - and once now, if already up.
    client.add_connect_callback(_refresh)
    if client.connected.is_set():
        await _refresh()
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

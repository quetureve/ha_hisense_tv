"""Support for Picture Settings and per-source signal status sensors."""
from datetime import timedelta
import json
from json.decoder import JSONDecodeError
import logging
from wakeonlan import BROADCAST_IP

from homeassistant.components.sensor import SensorEntity
from homeassistant.const import CONF_IP_ADDRESS, CONF_MAC, CONF_NAME
from homeassistant.util import dt as dt_util

from .const import DATA_CLIENT, DEFAULT_NAME, DOMAIN
from .helper import HisenseTvBase

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass, config_entry, async_add_entities):
    """Set up MQTT sensors dynamically through MQTT discovery."""
    _LOGGER.debug("async_setup_entry config: %s", config_entry.data)

    name = config_entry.data[CONF_NAME]
    mac = config_entry.data[CONF_MAC]
    ip_address = config_entry.data.get(CONF_IP_ADDRESS, BROADCAST_IP)
    client = hass.data[DOMAIN][config_entry.entry_id][DATA_CLIENT]
    uid = config_entry.unique_id
    if uid is None:
        uid = config_entry.entry_id

    entity = HisenseTvSensor(
        hass=hass,
        mqtt_client=client,
        name=name + " Picture Settings",
        mac=mac,
        uid=uid,
        ip_address=ip_address,
    )
    async_add_entities([entity])

    source_status = HisenseTvSourceStatusSensor(
        hass=hass,
        mqtt_client=client,
        name=name + " Source Status",
        mac=mac,
        uid=uid,
        ip_address=ip_address,
    )
    async_add_entities([source_status])


class HisenseTvSensor(SensorEntity, HisenseTvBase):
    """Representation of a sensor that can be updated using MQTT."""

    def __init__(self, hass, mqtt_client, name, mac, uid, ip_address):
        HisenseTvBase.__init__(
            self=self,
            hass=hass,
            mqtt_client=mqtt_client,
            name=name,
            mac=mac,
            uid=uid,
            ip_address=ip_address,
        )
        self._is_available = False
        self._state = {}
        self._last_trigger = dt_util.utcnow()
        self._force_trigger = False

    async def async_will_remove_from_hass(self):
        for unsubscribe in list(self._subscriptions.values()):
            unsubscribe()

    async def async_added_to_hass(self):
        self._subscriptions["tvsleep"] = await self._mqtt.async_subscribe(
            self._in_topic(
                "/remoteapp/mobile/broadcast/platform_service/actions/tvsleep"
            ),
            self._message_received_turnoff,
        )

        self._subscriptions["state"] = await self._mqtt.async_subscribe(
            self._in_topic("/remoteapp/mobile/broadcast/ui_service/state"),
            self._message_received_turnon,
        )

        self._subscriptions["picturesettings"] = await self._mqtt.async_subscribe(
            self._in_topic("/remoteapp/mobile/%s/platform_service/data/picturesetting"),
            self._message_received,
        )

        self._subscriptions["picturesettings_value"] = await self._mqtt.async_subscribe(
            self._in_topic(
                "/remoteapp/mobile/broadcast/platform_service/data/picturesetting"
            ),
            self._message_received_value,
        )

    async def _message_received_turnoff(self, msg):
        _LOGGER.debug("message_received_turnoff")
        self._is_available = False
        self.async_write_ha_state()

    async def _message_received_turnon(self, msg):
        _LOGGER.debug("message_received_turnon")
        if msg.retain:
            _LOGGER.debug("message_received_turnon - skip retained message")
            return

        self._is_available = True
        self._force_trigger = True
        self.async_write_ha_state()

    async def _message_received(self, msg):
        self._is_available = True
        try:
            payload = json.loads(msg.payload)
        except JSONDecodeError:
            payload = {}
        _LOGGER.debug("_message_received R(%s):\n%s", msg.retain, payload)
        self._state = {
            s.get("menu_id"): {"name": s.get("menu_name"), "value": s.get("menu_value")}
            for s in payload.get("menu_info", [])
        }
        self.async_write_ha_state()

    async def _message_received_value(self, msg):
        self._is_available = True
        self._force_trigger = True
        try:
            payload = json.loads(msg.payload)
        except JSONDecodeError:
            payload = {}
        _LOGGER.debug("_message_received_value R(%s):\n%s", msg.retain, payload)
        if "notify_value_changed" == payload.get("action"):
            menu_id = payload.get("menu_id")
            entry = self._state.get(menu_id)
            if entry is not None:
                entry["value"] = payload.get("menu_value")
            else:
                _LOGGER.debug("_message_received_value menu_id not found: %s", menu_id)

        self.async_write_ha_state()

    @property
    def name(self):
        """Return the name of the sensor."""
        return self._name

    @property
    def native_value(self):
        """Return the state of the sensor."""
        return self._state.get(91, {}).get("value", "")

    @property
    def available(self):
        """Return True if entity is available."""
        return self._is_available

    @property
    def icon(self):
        """Return the icon to use in the frontend."""
        return "mdi:palette"

    @property
    def extra_state_attributes(self):
        """Return the state attributes of the sensor."""
        return {v["name"]: v["value"] for k, v in self._state.items()}

    async def async_update(self):
        """Get the latest data and updates the states."""
        if (
            not self._force_trigger
            and dt_util.utcnow() - self._last_trigger < timedelta(minutes=1)
        ):
            _LOGGER.debug("Skip update")
            return

        _LOGGER.debug("Update. force=%s", self._force_trigger)
        self._force_trigger = False
        self._last_trigger = dt_util.utcnow()

        await self._mqtt.async_publish(
            self._out_topic("/remoteapp/tv/ui_service/%s/actions/gettvstate"),
            "",
            retain=False,
        )

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, self._unique_id)},
            "name": self._name.replace(" Picture Settings", ""),
            "manufacturer": DEFAULT_NAME,
        }

    @property
    def unique_id(self):
        """Return the unique id of the device."""
        return self._unique_id


class HisenseTvSourceStatusSensor(SensorEntity, HisenseTvBase):
    """Per-source HDMI signal status, built from the sourceinsert broadcast
    and refreshed from sourcelist whenever the TV turns on.

    Exposes each known HDMI source as an "on"/"off" attribute (e.g. "HDMI2":
    "on") so it can be used directly as a state trigger in an automation.
    Switching logic and name-to-source mapping are intentionally left out of
    the integration - that lives in a Home Assistant automation instead.
    """

    def __init__(self, hass, mqtt_client, name, mac, uid, ip_address):
        HisenseTvBase.__init__(
            self=self,
            hass=hass,
            mqtt_client=mqtt_client,
            name=name,
            mac=mac,
            uid=f"{uid}_source_status",
            ip_address=ip_address,
        )
        self._parent_uid = uid
        self._is_available = False
        self._sources = {}

    async def async_will_remove_from_hass(self):
        for unsubscribe in list(self._subscriptions.values()):
            unsubscribe()

    async def async_added_to_hass(self):
        self._subscriptions["sourceinsert"] = await self._mqtt.async_subscribe(
            self._in_topic(
                "/remoteapp/mobile/broadcast/platform_service/actions/sourceinsert"
            ),
            self._message_received_sourceinsert,
        )
        self._subscriptions["sourcelist"] = await self._mqtt.async_subscribe(
            self._in_topic("/remoteapp/mobile/%s/ui_service/data/sourcelist"),
            self._message_received_sourcelist,
        )
        self._subscriptions["state"] = await self._mqtt.async_subscribe(
            self._in_topic("/remoteapp/mobile/broadcast/ui_service/state"),
            self._message_received_turnon,
        )
        # The TV may already be on when this entity is added (e.g. after a
        # HA restart), so ask for the current picture right away instead of
        # only reacting to the next sourceinsert change.
        await self._request_sourcelist()

    async def _request_sourcelist(self):
        _LOGGER.debug("HisenseTvSourceStatusSensor: requesting sourcelist")
        await self._mqtt.async_publish(
            self._out_topic("/remoteapp/tv/ui_service/%s/actions/sourcelist"), ""
        )

    async def _message_received_turnon(self, msg):
        if msg.retain:
            return
        _LOGGER.debug("HisenseTvSourceStatusSensor: TV turned on")
        await self._request_sourcelist()

    def _update_sources(self, items):
        for item in items:
            raw_name = item.get("sourcename") or item.get("displayname") or ""
            port = raw_name.rsplit("-", 1)[0] if "-" in raw_name else raw_name
            if not port or not port.startswith("HDMI"):
                continue
            # sourceinsert sends is_signal as an int (1/0), sourcelist sends
            # it as a string ("1"/"0") - normalize both to compare the same way.
            is_signal = str(item.get("is_signal")) == "1"
            self._sources[port] = "on" if is_signal else "off"

    async def _message_received_sourceinsert(self, msg):
        self._is_available = True
        try:
            payload = json.loads(msg.payload)
        except JSONDecodeError:
            payload = []
        _LOGGER.debug(
            "_message_received_sourceinsert R(%s):\n%s", msg.retain, payload
        )
        self._update_sources(payload)
        self.async_write_ha_state()

    async def _message_received_sourcelist(self, msg):
        self._is_available = True
        try:
            payload = json.loads(msg.payload)
        except JSONDecodeError:
            payload = []
        _LOGGER.debug("_message_received_sourcelist R(%s):\n%s", msg.retain, payload)
        self._update_sources(payload)
        self.async_write_ha_state()

    @property
    def name(self):
        """Return the name of the sensor."""
        return self._name

    @property
    def native_value(self):
        """Quick summary: which ports currently have a signal."""
        active = sorted(p for p, v in self._sources.items() if v == "on")
        return ", ".join(active) if active else "No signal"

    @property
    def extra_state_attributes(self):
        """Per-port "on"/"off" signal status."""
        return dict(self._sources)

    @property
    def available(self):
        """Return True once at least one sourceinsert message has arrived."""
        return self._is_available

    @property
    def icon(self):
        return "mdi:hdmi-port"

    @property
    def should_poll(self):
        return False

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, self._parent_uid)},
            "name": self._name.replace(" Source Status", ""),
            "manufacturer": DEFAULT_NAME,
        }

    @property
    def unique_id(self):
        """Return the unique id of the device."""
        return self._unique_id

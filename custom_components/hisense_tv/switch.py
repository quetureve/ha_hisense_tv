"""Hisense TV switch entity"""
import logging
import wakeonlan
import json
from json.decoder import JSONDecodeError

from homeassistant.components.switch import SwitchEntity
from homeassistant.const import CONF_IP_ADDRESS, CONF_MAC, CONF_NAME

from .const import DATA_CLIENT, DEFAULT_NAME, DOMAIN
from .helper import HisenseTvBase

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass, config_entry, async_add_entities):
    """Start HisenseTV switch setup process."""
    _LOGGER.debug("async_setup_entry config: %s", config_entry.data)

    name = config_entry.data[CONF_NAME]
    mac = config_entry.data[CONF_MAC]
    ip_address = config_entry.data.get(CONF_IP_ADDRESS, wakeonlan.BROADCAST_IP)
    client = hass.data[DOMAIN][config_entry.entry_id][DATA_CLIENT]
    uid = config_entry.unique_id or config_entry.entry_id

    entity = HisenseTvSwitch(
        hass=hass,
        mqtt_client=client,
        name=name,
        mac=mac,
        uid=uid,
        ip_address=ip_address,
    )
    async_add_entities([entity])

    gamemode = HisenseGameModeSwitch(
        hass=hass,
        mqtt_client=client,
        name=name + " Game Mode",
        mac=mac,
        uid=uid,
        ip_address=ip_address,
    )
    async_add_entities([gamemode])


class HisenseTvSwitch(SwitchEntity, HisenseTvBase):
    """Hisense TV switch entity."""

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
        self._is_on = False

    async def async_turn_on(self, **kwargs):
        wakeonlan.send_magic_packet(self._mac, ip_address=self._ip_address)

    async def async_turn_off(self, **kwargs):
        await self._mqtt.async_publish(
            self._out_topic("/remoteapp/tv/remote_service/%s/actions/sendkey"),
            "KEY_POWER",
            retain=False,
        )

    @property
    def is_on(self):
        return self._is_on

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, self._unique_id)},
            "name": self._name,
            "manufacturer": DEFAULT_NAME,
        }

    @property
    def unique_id(self):
        return self._unique_id

    @property
    def name(self):
        return self._name

    @property
    def icon(self):
        return self._icon

    @property
    def should_poll(self):
        return False

    async def async_will_remove_from_hass(self):
        for unsubscribe in list(self._subscriptions.values()):
            unsubscribe()

    async def async_added_to_hass(self):
        self._subscriptions["tvsleep"] = await self._mqtt.async_subscribe(
            self._in_topic("/remoteapp/mobile/broadcast/platform_service/actions/tvsleep"),
            self._message_received_turnoff,
        )

        self._subscriptions["state"] = await self._mqtt.async_subscribe(
            self._in_topic("/remoteapp/mobile/broadcast/ui_service/state"),
            self._message_received_state,
        )

        self._subscriptions["volume"] = await self._mqtt.async_subscribe(
            self._in_topic("/remoteapp/mobile/broadcast/platform_service/actions/volumechange"),
            self._message_received_state,
        )

        self._subscriptions["sourcelist"] = await self._mqtt.async_subscribe(
            self._in_topic("/remoteapp/mobile/%s/ui_service/data/sourcelist"),
            self._message_received_state,
        )

    async def _message_received_turnoff(self, msg):
        _LOGGER.debug("message_received_turnoff")
        self._is_on = False
        self.async_write_ha_state()

    async def _message_received_state(self, msg):
        if msg.retain:
            _LOGGER.debug("SWITCH message_received_state - skip retained message")
            return

        _LOGGER.debug("SWITCH message_received_state - turn on")
        self._is_on = True
        self.async_write_ha_state()


class HisenseGameModeSwitch(SwitchEntity, HisenseTvBase):
    """Hisense GameMode switch entity."""

    def __init__(self, hass, mqtt_client, name, mac, uid, ip_address):
        HisenseTvBase.__init__(
            self=self,
            hass=hass,
            mqtt_client=mqtt_client,
            name=name,
            mac=mac,
            uid=f"{uid}_game_mode",
            ip_address=ip_address,
        )
        self._parent_uid = uid
        self._is_on = False
        self._is_available = True  # this switch has no separate "off" signal, so default to available

    async def async_will_remove_from_hass(self):
        for unsubscribe in list(self._subscriptions.values()):
            unsubscribe()

    async def async_added_to_hass(self):
        self._subscriptions["tvsleep"] = await self._mqtt.async_subscribe(
            self._in_topic("/remoteapp/mobile/broadcast/platform_service/actions/tvsleep"),
            self._message_received_turnoff,
        )

        self._subscriptions["state"] = await self._mqtt.async_subscribe(
            self._in_topic("/remoteapp/mobile/broadcast/ui_service/state"),
            self._message_received_turnon,
        )

        self._subscriptions["picturesettings_value"] = await self._mqtt.async_subscribe(
            self._in_topic("/remoteapp/mobile/broadcast/platform_service/data/picturesetting"),
            self._message_received_value,
        )

        # Mark as available on add
        self.async_write_ha_state()

    async def async_turn_on(self, **kwargs):
        await self._mqtt.async_publish(
            self._out_topic("/remoteapp/tv/platform_service/%s/actions/picturesetting"),
            '{"action":"set_value","menu_id":122,"menu_value_type":"int", "menu_value":1}',
            retain=False,
        )

    async def async_turn_off(self, **kwargs):
        await self._mqtt.async_publish(
            self._out_topic("/remoteapp/tv/platform_service/%s/actions/picturesetting"),
            '{"action":"set_value","menu_id":122,"menu_value_type":"int", "menu_value":0}',
            retain=False,
        )

    async def _message_received_turnon(self, msg):
        _LOGGER.debug("message_received_turnon")
        if msg.retain:
            _LOGGER.debug("message_received_turnon - skip retained message")
            return

        self._is_available = True
        self.async_write_ha_state()

        await self._mqtt.async_publish(
            self._out_topic("/remoteapp/tv/platform_service/%s/actions/picturesetting"),
            '{"action": "get_menu_info"}',
            retain=False,
        )

    async def _message_received_value(self, msg):
        self._is_available = True
        try:
            payload = json.loads(msg.payload)
        except JSONDecodeError:
            payload = {}
        _LOGGER.debug("_message_received_value R(%s):\n%s", msg.retain, payload)
        if payload.get("action") == "notify_value_changed" and payload.get("menu_id") == 122:
            self._is_on = payload.get("menu_value") == 1
        elif payload.get("action") == "resp_get_menu_info":
            for s in payload.get("menu_info", []):
                if s.get("menu_id") == 122:
                    self._is_on = s.get("menu_value") == 1
        self.async_write_ha_state()

    async def _message_received_turnoff(self, msg):
        _LOGGER.debug("message_received_turnoff")
        self._is_available = False
        self.async_write_ha_state()

    @property
    def is_on(self):
        return self._is_on

    @property
    def available(self):
        return self._is_available

    @property
    def unique_id(self):
        return self._unique_id

    @property
    def name(self):
        return self._name

    @property
    def icon(self):
        return "mdi:gamepad-variant"

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, self._parent_uid)},
            "name": self._name.replace(" Game Mode", ""),
            "manufacturer": DEFAULT_NAME,
        }

    @property
    def should_poll(self):
        return False

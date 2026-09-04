"""Hisense TV integration helper methods."""
import asyncio
import logging

from .const import DEFAULT_CLIENT_ID

_LOGGER = logging.getLogger(__name__)


async def mqtt_pub_sub(client, pub, sub, payload=""):
    """Wrapper for publishing a topic and receiving replies on a subscribed topic."""
    queue = asyncio.Queue()

    async def put(message):
        await queue.put(message)

    async def get():
        while True:
            yield await asyncio.wait_for(queue.get(), timeout=10)

    unsubscribe = await client.async_subscribe(sub, put)
    await client.async_publish(pub, payload)
    return get(), unsubscribe


class HisenseTvBase:
    """Hisense TV base entity."""

    def __init__(
        self,
        hass,
        mqtt_client,
        name: str,
        mac: str,
        uid: str,
        ip_address: str,
    ):
        self._hass = hass
        self._mqtt = mqtt_client
        self._client_id = DEFAULT_CLIENT_ID
        self._name = name
        self._mac = mac
        self._ip_address = ip_address
        self._unique_id = uid
        self._icon = "mdi:television-shimmer"
        self._subscriptions = {
            "tvsleep": lambda: None,
            "state": lambda: None,
            "volume": lambda: None,
            "sourcelist": lambda: None,
        }

    def _topic(self, template=""):
        """Fill in the "%s" client-id placeholder used by the TV's topics.

        There is no local/remote prefix to add anymore - this integration
        talks directly to the TV's own broker - so both helpers below just
        do the same substitution. They're kept as two names only so the
        rest of the codebase (which calls _in_topic for incoming data and
        _out_topic for outgoing actions) didn't need to change.
        """
        try:
            return template % self._client_id
        except TypeError:
            return template

    def _out_topic(self, template=""):
        return self._topic(template)

    def _in_topic(self, template=""):
        return self._topic(template)

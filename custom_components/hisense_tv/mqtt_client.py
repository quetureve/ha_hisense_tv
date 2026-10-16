"""Native MQTT client for talking directly to a Hisense TV's built-in broker.

This replaces the previous approach of bridging the TV's on-device broker
into Home Assistant's core `mqtt` integration via an external Mosquitto
`connection` block. Home Assistant is no longer involved in the MQTT
transport at all - this integration owns its own connection, using the same
TLS certificate files you already have on disk.

paho-mqtt's network loop and callbacks run on their own background thread;
every callback here hands off to the Home Assistant event loop so entities
never have to deal with thread safety themselves.
"""
import asyncio
from dataclasses import dataclass
import logging
from collections.abc import Awaitable, Callable
import ssl
from typing import Optional

import paho.mqtt.client as paho

_LOGGER = logging.getLogger(__name__)

# paho pings after one idle keepalive interval and then waits one more for the
# answer, so a TV that vanished is noticed after up to 2x this value (the old
# value of 60 meant up to ~2 minutes).
KEEPALIVE = 20
# paho's default reconnect backoff doubles up to 120 s. While the TV is off the
# client keeps retrying, so after power-on the next attempt could be two
# minutes away. Cap it so a reconnect happens within seconds.
RECONNECT_MIN_DELAY = 1
RECONNECT_MAX_DELAY = 10


@dataclass
class Message:
    """A received MQTT message.

    Mirrors the shape of homeassistant.components.mqtt's message object
    (topic / payload / retain) so the rest of the integration's message
    handlers don't need to change.
    """

    topic: str
    payload: str
    retain: bool = False


MessageCallback = Callable[[Message], Awaitable[None]]
ConnectCallback = Callable[[], Awaitable[None]]


class HisenseMqttError(Exception):
    """Raised when the client cannot connect to the TV's broker."""


class HisenseMqttClient:
    """Wraps paho-mqtt with the TLS setup this integration needs."""

    def __init__(
        self,
        hass,
        host: str,
        port: int,
        username: str,
        password: str,
        client_id: str,
        ca_cert: Optional[str] = None,
        client_cert: Optional[str] = None,
        client_key: Optional[str] = None,
    ):
        self._hass = hass
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._client_id = client_id
        self._ca_cert = ca_cert
        self._client_cert = client_cert
        self._client_key = client_key
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._subscriptions: dict[str, list[MessageCallback]] = {}
        self._connect_callbacks: list[ConnectCallback] = []
        self.connected = asyncio.Event()
        self._client: Optional[paho.Client] = None

    def _build_paho_client(self) -> paho.Client:
        """Build the paho client, including TLS setup.

        This does blocking file I/O (reading the certificate files) and
        crypto work, so it must only ever run inside an executor job, never
        directly on the event loop.
        """
        client = paho.Client(
            callback_api_version=paho.CallbackAPIVersion.VERSION2,
            client_id=self._client_id,
            protocol=paho.MQTTv311,
            clean_session=True,
        )
        client.username_pw_set(self._username, self._password)
        if self._ca_cert:
            # Mirrors a working Mosquitto bridge block using the same three
            # files: bridge_tls_version tlsv1.2 / bridge_insecure true.
            client.tls_set(
                ca_certs=self._ca_cert,
                certfile=self._client_cert,
                keyfile=self._client_key,
                tls_version=ssl.PROTOCOL_TLSv1_2,
            )
            client.tls_insecure_set(True)

        client.reconnect_delay_set(
            min_delay=RECONNECT_MIN_DELAY, max_delay=RECONNECT_MAX_DELAY
        )
        client.on_connect = self._on_connect
        client.on_connect_fail = self._on_connect_fail
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        return client

    async def async_connect(
        self, timeout: float = 15.0, required: bool = True
    ) -> None:
        """Open the connection.

        required=True: raise HisenseMqttError if the broker can't be reached
        (the config flow needs this to report problems to the user).

        required=False: never fail just because the TV is off - keep retrying
        in the background and connect as soon as it is reachable, so Home
        Assistant doesn't need its own (slow, growing) setup-retry cycle.
        Problems that can't fix themselves, such as unreadable certificate
        files, still raise.
        """
        self._loop = asyncio.get_running_loop()

        def _connect():
            self._client = self._build_paho_client()
            if required:
                self._client.connect(self._host, self._port, keepalive=KEEPALIVE)
            else:
                self._client.connect_async(
                    self._host, self._port, keepalive=KEEPALIVE
                )
            self._client.loop_start()

        try:
            await self._hass.async_add_executor_job(_connect)
        except Exception as err:
            # Bad cert paths, a malformed cert/key, DNS failure, connection
            # refused, etc. all land here - surface them uniformly so the
            # config flow's except clause (and __init__.py's) can catch it
            # instead of an unhandled exception crashing the flow/setup.
            raise HisenseMqttError(
                f"Could not connect to {self._host}:{self._port}: {err}"
            ) from err

        try:
            await asyncio.wait_for(self.connected.wait(), timeout=timeout)
        except asyncio.TimeoutError as err:
            if required:
                await self.async_disconnect()
                raise HisenseMqttError(
                    f"Timed out connecting to {self._host}:{self._port}"
                ) from err
            _LOGGER.info(
                "TV broker at %s:%s is not reachable yet (TV off?) - "
                "will connect in the background as soon as it is up",
                self._host,
                self._port,
            )

    def add_connect_callback(self, callback: ConnectCallback) -> Callable[[], None]:
        """Call `callback` every time the connection is (re)established.

        Returns a function that removes the callback again.
        """
        self._connect_callbacks.append(callback)

        def _remove() -> None:
            if callback in self._connect_callbacks:
                self._connect_callbacks.remove(callback)

        return _remove

    async def async_disconnect(self) -> None:
        if self._client is None:
            return

        def _disconnect():
            self._client.loop_stop()
            self._client.disconnect()

        await self._hass.async_add_executor_job(_disconnect)

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            _LOGGER.error("HisenseMqttClient connect failed: %s", reason_code)
            return
        _LOGGER.info("Connected to the TV's broker at %s:%s", self._host, self._port)
        for topic in list(self._subscriptions):
            client.subscribe(topic)
        self._loop.call_soon_threadsafe(self.connected.set)
        # Subscriptions are sent first, so the TV's answers to whatever the
        # callbacks ask for (state, volume, sources...) will reach the entities.
        for callback in list(self._connect_callbacks):
            asyncio.run_coroutine_threadsafe(callback(), self._loop)

    def _on_connect_fail(self, client, userdata):
        # Fires on every failed attempt while the TV is off - debug only.
        _LOGGER.debug(
            "Connection attempt to %s:%s failed, will retry", self._host, self._port
        )

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        _LOGGER.info("Disconnected from the TV's broker: %s", reason_code)
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self.connected.clear)

    def _on_message(self, client, userdata, msg):
        message = Message(
            topic=msg.topic,
            payload=msg.payload.decode("utf-8", errors="replace"),
            retain=bool(msg.retain),
        )
        for callback in list(self._subscriptions.get(msg.topic, [])):
            asyncio.run_coroutine_threadsafe(callback(message), self._loop)

    async def async_subscribe(
        self, topic: str, msg_callback: MessageCallback
    ) -> Callable[[], None]:
        """Subscribe to a topic. Returns a function that unsubscribes."""
        is_new_topic = topic not in self._subscriptions
        self._subscriptions.setdefault(topic, []).append(msg_callback)
        if is_new_topic:
            await self._hass.async_add_executor_job(self._client.subscribe, topic)

        def _unsubscribe():
            callbacks = self._subscriptions.get(topic)
            if callbacks and msg_callback in callbacks:
                callbacks.remove(msg_callback)
            if not callbacks:
                self._subscriptions.pop(topic, None)
                self._client.unsubscribe(topic)

        return _unsubscribe

    async def async_publish(
        self, topic: str, payload: str = "", retain: bool = False
    ) -> None:
        """Publish and wait for the call to be handed to the network thread."""
        await self._hass.async_add_executor_job(
            self._client.publish, topic, payload, 0, retain
        )

    def publish(self, topic: str, payload: str = "", retain: bool = False) -> None:
        """Fire-and-forget publish for use from a sync callback."""
        self._client.publish(topic, payload, 0, retain)

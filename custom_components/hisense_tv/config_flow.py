"""Hisense TV config flow."""
import json
from json.decoder import JSONDecodeError
import logging
from pathlib import Path
import shutil

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.components.file_upload import process_uploaded_file
from homeassistant.config_entries import ConfigFlowResult
from homeassistant.const import CONF_IP_ADDRESS, CONF_MAC, CONF_NAME, CONF_PIN
from homeassistant.helpers.selector import FileSelector, FileSelectorConfig
from homeassistant.helpers.storage import STORAGE_DIR

from .const import (
    CONF_MQTT_PORT,
    CONF_TLS_CA_CERT,
    CONF_TLS_CLIENT_CERT,
    CONF_TLS_CLIENT_KEY,
    DEFAULT_CLIENT_ID,
    DEFAULT_MQTT_PASSWORD,
    DEFAULT_MQTT_PORT,
    DEFAULT_MQTT_USERNAME,
    DEFAULT_NAME,
    DOMAIN,
)
from .mqtt_client import HisenseMqttClient, HisenseMqttError

_LOGGER = logging.getLogger(__name__)

# Upload-widget field names. Kept separate from the CONF_TLS_* keys the rest
# of the integration reads, because those store a permanent file *path* once
# an upload has been processed - not the upload itself.
CONF_TLS_CA_CERT_UPLOAD = "tls_ca_cert_upload"
CONF_TLS_CLIENT_CERT_UPLOAD = "tls_client_cert_upload"
CONF_TLS_CLIENT_KEY_UPLOAD = "tls_client_key_upload"

# upload field -> (stored path key, permanent filename)
_UPLOAD_FIELDS = {
    CONF_TLS_CA_CERT_UPLOAD: (CONF_TLS_CA_CERT, "ca.crt"),
    CONF_TLS_CLIENT_CERT_UPLOAD: (CONF_TLS_CLIENT_CERT, "client.crt"),
    CONF_TLS_CLIENT_KEY_UPLOAD: (CONF_TLS_CLIENT_KEY, "client.key"),
}


async def _async_store_uploaded_file(hass, uploaded_file_id: str, filename: str) -> str:
    """Copy an uploaded file into permanent storage under .storage/hisense_tv/.

    Runs the actual file I/O in an executor - process_uploaded_file/shutil
    are blocking calls and must never run directly on the event loop.
    """
    dest_dir = Path(hass.config.path(STORAGE_DIR, DOMAIN))

    def _process() -> str:
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / filename
        with process_uploaded_file(hass, uploaded_file_id) as src_path:
            shutil.copy(src_path, dest)
        return str(dest)

    return await hass.async_add_executor_job(_process)


def _data_schema(defaults=None):
    """Build the setup/reconfigure form, pre-filled from `defaults` if given.

    The three certificate fields are always shown empty - on a reconfigure,
    leaving one blank keeps the file already on file for this entry.
    """
    defaults = defaults or {}
    return vol.Schema(
        {
            vol.Required(
                CONF_NAME, default=defaults.get(CONF_NAME, DEFAULT_NAME)
            ): str,
            vol.Required(CONF_MAC, default=defaults.get(CONF_MAC, "")): str,
            vol.Required(
                CONF_IP_ADDRESS, default=defaults.get(CONF_IP_ADDRESS, "")
            ): str,
            vol.Optional(
                CONF_MQTT_PORT,
                default=defaults.get(CONF_MQTT_PORT, DEFAULT_MQTT_PORT),
            ): int,
            vol.Optional(CONF_TLS_CA_CERT_UPLOAD): FileSelector(
                FileSelectorConfig(accept=".crt,.pem,.cer")
            ),
            vol.Optional(CONF_TLS_CLIENT_CERT_UPLOAD): FileSelector(
                FileSelectorConfig(accept=".crt,.pem,.cer")
            ),
            vol.Optional(CONF_TLS_CLIENT_KEY_UPLOAD): FileSelector(
                FileSelectorConfig(accept=".key,.pem,.pkcs8")
            ),
        }
    )


class HisenseTvFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Hisense TV config flow."""

    VERSION = 2

    def __init__(self):
        """Initialize the config flow."""
        self.task_mqtt = None
        self._client = None
        self._auth_task = None
        self._authcode_task = None

    def _build_client(self) -> HisenseMqttClient:
        return HisenseMqttClient(
            hass=self.hass,
            host=self.task_mqtt[CONF_IP_ADDRESS],
            port=self.task_mqtt.get(CONF_MQTT_PORT, DEFAULT_MQTT_PORT),
            username=DEFAULT_MQTT_USERNAME,
            password=DEFAULT_MQTT_PASSWORD,
            client_id=DEFAULT_CLIENT_ID,
            ca_cert=self.task_mqtt.get(CONF_TLS_CA_CERT) or None,
            client_cert=self.task_mqtt.get(CONF_TLS_CLIENT_CERT) or None,
            client_key=self.task_mqtt.get(CONF_TLS_CLIENT_KEY) or None,
        )

    async def _wait_for_authentication(self):
        """Connect, ask the TV for its state, and see whether it demands a PIN.

        Resolves to True if the TV answered with a source list directly (no
        PIN needed), or False if it asked for authentication first. Raises
        HisenseMqttError if the broker connection itself fails.
        """
        self._client = self._build_client()
        await self._client.async_connect()

        result = self.hass.loop.create_future()

        async def _pin_needed(message):
            if not result.done():
                result.set_result(False)

        async def _pin_not_needed(message):
            if not result.done():
                result.set_result(True)

        unsub_auth = await self._client.async_subscribe(
            "/remoteapp/mobile/%s/ui_service/data/authentication" % DEFAULT_CLIENT_ID,
            _pin_needed,
        )
        unsub_sourcelist = await self._client.async_subscribe(
            "/remoteapp/mobile/%s/ui_service/data/sourcelist" % DEFAULT_CLIENT_ID,
            _pin_not_needed,
        )
        try:
            await self._client.async_publish(
                "/remoteapp/tv/ui_service/%s/actions/gettvstate" % DEFAULT_CLIENT_ID
            )
            await self._client.async_publish(
                "/remoteapp/tv/ui_service/%s/actions/sourcelist" % DEFAULT_CLIENT_ID
            )
            return await result
        finally:
            unsub_auth()
            unsub_sourcelist()

    async def _wait_for_authcode_response(self, pin):
        """Send the PIN to the TV and wait for it to accept or reject it."""
        result = self.hass.loop.create_future()

        async def _authcode_response(message):
            if result.done():
                return
            try:
                payload = json.loads(message.payload)
            except JSONDecodeError:
                payload = {}
            _LOGGER.debug("_wait_for_authcode_response %s", payload)
            result.set_result(payload.get("result") == 1)

        unsub = await self._client.async_subscribe(
            "/remoteapp/mobile/%s/ui_service/data/authenticationcode"
            % DEFAULT_CLIENT_ID,
            _authcode_response,
        )
        try:
            await self._client.async_publish(
                "/remoteapp/tv/ui_service/%s/actions/authenticationcode"
                % DEFAULT_CLIENT_ID,
                json.dumps({"authNum": pin}),
            )
            return await result
        finally:
            unsub()

    async def async_step_user(self, user_input=None) -> ConfigFlowResult:
        if self.task_mqtt is None:
            if user_input is None:
                reconfigure_defaults = None
                if self.source == config_entries.SOURCE_RECONFIGURE:
                    reconfigure_defaults = self._get_reconfigure_entry().data
                _LOGGER.debug("async_step_user - user_input is None")
                return self.async_show_form(
                    step_id="user", data_schema=_data_schema(reconfigure_defaults)
                )
            _LOGGER.debug("async_step_user - set task_mqtt")
            existing = (
                self._get_reconfigure_entry().data
                if self.source == config_entries.SOURCE_RECONFIGURE
                else {}
            )
            task_mqtt = {
                CONF_NAME: user_input[CONF_NAME],
                CONF_MAC: user_input[CONF_MAC],
                CONF_IP_ADDRESS: user_input[CONF_IP_ADDRESS],
                CONF_MQTT_PORT: user_input.get(CONF_MQTT_PORT, DEFAULT_MQTT_PORT),
            }
            for upload_key, (stored_key, filename) in _UPLOAD_FIELDS.items():
                uploaded_file_id = user_input.get(upload_key)
                if uploaded_file_id:
                    task_mqtt[stored_key] = await _async_store_uploaded_file(
                        self.hass, uploaded_file_id, filename
                    )
                else:
                    task_mqtt[stored_key] = existing.get(stored_key)
            self.task_mqtt = task_mqtt

        if self._auth_task is None:
            self._auth_task = self.hass.async_create_task(
                self._wait_for_authentication()
            )

        if not self._auth_task.done():
            return self.async_show_progress(
                step_id="user",
                progress_action="progress_action",
                progress_task=self._auth_task,
            )

        auth_task, self._auth_task = self._auth_task, None
        try:
            no_auth_needed = auth_task.result()
        except HisenseMqttError:
            self.task_mqtt = None
            return self.async_show_progress_done(next_step_id="cannot_connect")

        return self.async_show_progress_done(
            next_step_id="finish" if no_auth_needed else "auth"
        )

    async def async_step_cannot_connect(self, user_input=None) -> ConfigFlowResult:
        """Could not reach the TV's broker - show the form again with an error."""
        reconfigure_defaults = None
        if self.source == config_entries.SOURCE_RECONFIGURE:
            reconfigure_defaults = self._get_reconfigure_entry().data
        return self.async_show_form(
            step_id="user",
            data_schema=_data_schema(reconfigure_defaults),
            errors={"base": "cannot_connect"},
        )

    async def async_step_reconfigure(self, user_input=None) -> ConfigFlowResult:
        """Reconfigure an existing entry - reuses the setup steps."""
        return await self.async_step_user(user_input)

    async def async_step_auth(self, user_input=None) -> ConfigFlowResult:
        """Auth handler."""
        if self._authcode_task is None:
            if user_input is None:
                _LOGGER.debug("async_step_auth - user_input is None -> show form")
                return self.async_show_form(
                    step_id="auth",
                    data_schema=vol.Schema(
                        {
                            vol.Required(CONF_PIN): int,
                        }
                    ),
                )

            _LOGGER.debug("async_step_auth send authentication: %s", user_input)
            self._authcode_task = self.hass.async_create_task(
                self._wait_for_authcode_response(user_input.get(CONF_PIN))
            )

        if not self._authcode_task.done():
            return self.async_show_progress(
                step_id="auth",
                progress_action="progress_action",
                progress_task=self._authcode_task,
            )

        auth_ok = self._authcode_task.result()
        self._authcode_task = None
        return self.async_show_progress_done(
            next_step_id="finish" if auth_ok else "auth"
        )

    async def async_step_finish(self, user_input=None) -> ConfigFlowResult:
        """Finish config flow."""
        _LOGGER.debug("async_step_finish")
        if self._client is not None:
            await self._client.async_disconnect()
            self._client = None

        if self.source == config_entries.SOURCE_RECONFIGURE:
            return self.async_update_reload_and_abort(
                self._get_reconfigure_entry(),
                data=self.task_mqtt,
                reason="reconfigure_successful",
            )
        return self.async_create_entry(
            title=self.task_mqtt[CONF_NAME], data=self.task_mqtt
        )

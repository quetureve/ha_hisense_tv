# Hisense TV

Home Assistant custom integration for controlling Hisense/VIDAA TVs over MQTT.

This connects **directly** to the TV's own on-device MQTT broker with mutual
TLS - no external Mosquitto bridge and no configured `mqtt:` integration
required. Everything (name, MAC, broker port, certificates) is set up
through the UI.

## Features

- **Media player** - power (Wake-on-LAN), volume, mute, input source
  selection, channel and app browsing.
- **Picture Settings sensor** - exposes the TV's picture menu items as
  attributes.
- **Source Status sensor** - per-HDMI-port signal status (`"HDMI2": "on"`),
  meant to be read from your own automations for input auto-switching or
  similar. This integration deliberately does **not** decide when to switch
  inputs - that logic belongs in an automation you write, since only you
  know which port maps to which device.
- **Switches** - power and Game Mode toggles.
- Config flow with certificate upload and a **Reconfigure** option (gear
  icon) to change any setting later without removing the integration.

## Requirements

- Home Assistant 2026.8 or newer.
- The TV's IP address and MAC address.
- The CA certificate, client certificate, and client key used to
  authenticate to the TV's on-device MQTT broker (port 36669 by default).
  Some TV models don't require these at all - leave the fields empty in
  that case. Where to obtain these files for your model is outside the
  scope of this integration; see the wider Home-Assistant-and-Hisense
  community for that.

## Installation

### HACS

1. HACS -> the three-dot menu -> **Custom repositories**.
2. Add this repository URL, category **Integration**.
3. Install "Hisense TV", then restart Home Assistant.

### Manual

Copy the `custom_components/hisense_tv` folder from this repository into
your Home Assistant `config/custom_components/` folder, then restart Home
Assistant.

## Configuration

Settings -> Devices & services -> Add integration -> **Hisense TV**.

| Field | Notes |
|---|---|
| Name | Used to name the device and its entities. |
| MAC address | Used for Wake-on-LAN when turning the TV on. |
| IP address | The TV's current IP address; also used as the broker host. |
| Broker port | Defaults to `36669`. |
| CA / client certificate / client key | Upload the files directly; leave empty if your TV doesn't require them. |

The TV will show a PIN on screen the first time you pair; enter it on the
next step. To change any of these later, use **Reconfigure** from the
integration's gear-icon menu - certificate fields left empty keep whatever
was uploaded previously.

## Using the Source Status sensor in an automation

The sensor exposes one attribute per HDMI port. For example, to switch to a
console when it powers on:

```yaml
trigger:
  - platform: state
    entity_id: sensor.hisense_tv_source_status
    attribute: HDMI2
    to: "on"
action:
  - service: media_player.select_source
    target:
      entity_id: media_player.hisense_tv
    data:
      source: HDMI2
```

## Troubleshooting

Enable debug logging in `configuration.yaml`:

```yaml
logger:
  logs:
    custom_components.hisense_tv: debug
```

A `FileNotFoundError` or connection error when adding the integration
almost always means the broker IP, port, or certificate files don't match
what the TV expects - check the debug log for the exact address and files
being used.

## Credits

Originally based on [sehaas/ha_hisense_tv](https://github.com/sehaas/ha_hisense_tv).

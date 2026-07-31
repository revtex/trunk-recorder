#!/bin/bash
#
# macOS launcher script for a Trunk Recorder instance.
#
# Wraps trunk-recorder in `caffeinate` to keep macOS from putting the
# machine — and, critically, the USB controller your SDR is attached to —
# into a low-power/suspended state. Without this, a launchd-managed
# trunk-recorder can silently lose its SDR after hours or days: macOS
# suspends the USB controller, the data channel dies, but the process keeps
# running at high CPU while producing no recordings and no log output.
#
#   caffeinate -i : prevent idle system sleep
#             -s : prevent system sleep while on AC power
#             -u : declare the system "user active" (keeps controllers awake)
#
# This is most important for USRP/UHD (B200 etc.) sources, which — unlike
# RTL-SDR — do not set the IOKit PowerOverrideOn flag that stops macOS from
# power-managing the device.
#
# Copy this to your trunk-build directory as start-<system>.sh, point the
# LaunchAgent plist at it, and set ProcessType=Interactive in that plist
# (see example-launchagent.plist).

ulimit -c unlimited
eval "$(/opt/homebrew/bin/brew shellenv)"

# Only needed for USRP/UHD-based SDRs — remove if using RTL-SDR or other drivers
export UHD_IMAGES_DIR=/usr/share/uhd/images/

exec caffeinate -isu ./trunk-recorder --config=config-<system>.json

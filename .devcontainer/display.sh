#!/bin/bash
pgrep -x Xvfb >/dev/null || (setsid Xvfb :99 -screen 0 1024x768x24 >/dev/null 2>&1 &)
sleep 1
pgrep -x x11vnc >/dev/null || (setsid x11vnc -display :99 -forever -nopw -quiet >/dev/null 2>&1 &)
pgrep -f websockify >/dev/null || (setsid websockify --web /usr/share/novnc 6080 localhost:5900 >/dev/null 2>&1 &)

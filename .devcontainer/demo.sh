#!/bin/bash
BIN=${1:-ft_bin}
cd /workspace
export DISPLAY=:99
bash .devcontainer/display.sh
if [ ! -x "$BIN/aquarium" ] || [ ! -x "$BIN/fish" ]; then
    echo "Missing $BIN/aquarium or $BIN/fish"
    exit 1
fi
make > /dev/null || exit 1
pkill -f "$BIN/aquarium"; pkill -f "$BIN/fish"; pkill -x server
sleep 1
./server > /tmp/server.out 2>/tmp/server.err &
sleep 1
IPC=$(head -1 /tmp/server.out)
./client "$IPC" create fishtank || exit 1
./$BIN/aquarium ./client "$IPC" fishtank viewer > /tmp/aquarium.out 2>&1 &
sleep 2
./$BIN/fish ./client "$IPC" fishtank > /tmp/fish.out 2>&1 &
echo "IPC: $IPC"
echo "Open: http://localhost:6080/vnc.html?autoconnect=true&resize=scale"
echo "Stop: pkill -f $BIN/aquarium; pkill -f $BIN/fish; pkill -x server"

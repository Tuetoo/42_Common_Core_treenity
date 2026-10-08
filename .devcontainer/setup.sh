#!/bin/bash
set -e
SRC=/etc/apt/sources.list.d/ubuntu.sources
if [ "$(uname -m)" = aarch64 ]; then
    dpkg --add-architecture amd64
    grep -q '^Architectures: arm64' "$SRC" || sed -i '/^Types: deb$/a Architectures: arm64' "$SRC"
    cat > /etc/apt/sources.list.d/amd64.sources <<'EOF'
Types: deb
URIs: http://archive.ubuntu.com/ubuntu/
Suites: noble noble-updates noble-security
Components: main universe
Architectures: amd64
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg
EOF
fi
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq xvfb x11vnc novnc websockify
if [ "$(uname -m)" = aarch64 ]; then
    VER=$(apt-cache policy libxml2:amd64 | awk '/Candidate:/ {print $2}')
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --allow-downgrades \
        libxml2="$VER" libxml2:amd64="$VER" libc6:amd64 libx11-6:amd64 \
        libxrandr2:amd64 libxcursor1:amd64 libxinerama1:amd64 libxi6:amd64 \
        libxxf86vm1:amd64 libgl1:amd64 libgl1-mesa-dri:amd64
fi
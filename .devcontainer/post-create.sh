#! /bin/bash

# Configure Docker daemon DNS before starting any containers
# Override DOCKER_DNS_1 / DOCKER_DNS_2 via remoteEnv in devcontainer.json
# DNS_1="${DOCKER_DNS_1:-195.83.24.30}"
# DNS_2="${DOCKER_DNS_2:-152.77.1.22}"
# DNS_3="${DOCKER_DNS_3:-8.8.8.8}"
# echo "{\"dns\": [\"${DNS_1}\", \"${DNS_2}\", \"${DNS_3}\"]}" | sudo tee /etc/docker/daemon.json > /dev/null

# docker-in-docker starts dockerd directly (not via a service), so we need to
# kill the running daemon and restart it for daemon.json to be picked up.
# sudo pkill dockerd || true
# sleep 2
# sudo dockerd &>/var/log/docker.log &

# # Wait for Docker to be ready (up to 60s)
# timeout 60 sh -c 'until docker info > /dev/null 2>&1; do sleep 1; done'

# # Workaround for newer linux kernel 
# # https://github.com/devcontainers/features/issues/1235#event-21749942947
# set -ex
# if ! docker info > /dev/null 2>&1; then
#     sudo update-alternatives --set iptables /usr/sbin/iptables-nft
# fi

# # Workaround for docker in docker config being set to a wrong value by default 
# rm -f ~/.docker/config.json

uv sync --all-groups
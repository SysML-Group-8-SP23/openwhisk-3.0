#!/bin/bash

# DO PER CONTAINER YOU WANT TO SPIN UP
NETWORK_NAME=$1
CONTAINER_RATE=$2
# creating a docker network
echo "NETWORK_NAME: ${NETWORK_NAME}"
echo "CONTAINER_RATE: ${CONTAINER_RATE}"
docker network create -d bridge "$NETWORK_NAME"

# get the network interface name
NETWORK_ID=$(docker network inspect -f {{.Id}} "$NETWORK_NAME")
NETWORK_ID="${NETWORK_ID:0:12}"
echo "NETWORK_ID: ${NETWORK_ID}"
CONTAINER_INTERFACE="br-${NETWORK_ID:0:12}"
echo "CONTAINER_INTERFACE: ${CONTAINER_INTERFACE}"

# throttle network interface
tc qdisc add dev "$CONTAINER_INTERFACE" root handle 1: htb default 11
tc class add dev "$CONTAINER_INTERFACE" parent 1: classid 1:11 htb rate "${CONTAINER_RATE}"

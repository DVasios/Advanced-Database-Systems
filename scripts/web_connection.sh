#! /bin/bash

param=$1;

# Allow Incoming Connections
if [[ "$param" == "-allow" ]]; then
    ufw allow 9870
    ufw allow 8088
    ufw allow 18080
fi

# # Deny Incoming Connections
if [[ "$param" == "-deny" ]]; then
    ufw deny 9870
    ufw deny 8088
    ufw deny 18080
fi
#!/usr/bin/env bash
# usage: ./scripts/run_tunnel.sh <ec2-public-ip> </path/to/key.pem>
set -euo pipefail
EC2_IP="${1:?provide EC2 public IP}"
KEY="${2:?provide path to pem key}"
ssh -i "$KEY" -N -L 8080:localhost:8080 "ubuntu@${EC2_IP}"


#!/bin/bash

# sudo su

# bash ./install_aws_cli.sh

curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"

unzip awscliv2.zip

./aws/install -i /usr/local/aws-cli -b /usr/local/bin
#!/bin/bash

# NOTE(cmo): Copy things into place. Will need sudo

cp mag /usr/local/bin/.
cp PrintMessagesInflux.py /usr/local/bin/.

cp magnetometer.service /etc/systemd/system/.
cp magnetometer_message_server.service /etc/systemd/system/.
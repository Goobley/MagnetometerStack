#!/bin/bash

gcc -c -O2 mqtt_pal.c mqtt.c
gcc -O2 -Wall -std=c99 magnetometer.c mqtt_pal.o mqtt.o -DHRDL_TEST -g -o mag
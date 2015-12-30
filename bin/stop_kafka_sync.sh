#!/bin/sh

ps ax | grep kafka_sync | grep -v grep | awk '{print $1}' | xargs kill -SIGTERM

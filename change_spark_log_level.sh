#!/bin/bash

LOG_LEVEL=$1
LOG4J_PATH="/usr/lib/spark/conf/log4j2.properties"
sed -i'' -e "s/rootLogger.level = .*/rootLogger.level = $LOG_LEVEL/" "$LOG4J_PATH"
echo "Spark log level set to $LOG_LEVEL"

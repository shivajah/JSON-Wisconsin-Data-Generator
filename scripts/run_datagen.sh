#!/bin/bash
configFile=$1
if [ -z $DATAGEN_HOME ]
then
	echo "ERROR: DATAGEN_HOME is not defined."
	exit 1
fi

CONFIGFILE=${DATAGEN_HOME}/src/main/java/com/datagen/configs/${configFile}
if [ ! -f ${CONFIGFILE} ]; then
	echo -e "ERROR: The configuration file for DataGen (with the name datagen-conf.json ) can not be found under ${DATAGEN_HOME}/src/main/java/com/datagen/configs directory."
	exit 1
fi

java -cp ${DATAGEN_HOME}/target/datagen-driver-jar-with-dependencies.jar com.datagen.Server ${DATAGEN_HOME} ${CONFIGFILE}

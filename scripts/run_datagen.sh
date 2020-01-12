#!/bin/bash

if [ -z $DATAGEN_HOME ]
then
	echo "ERROR: DATAGEN_HOME is not defined."
	exit 1
fi

java -cp ${DATAGEN_HOME}/target/datagen-driver-jar-with-dependencies.jar com.datagen.Server ${DATAGEN_HOME} $@

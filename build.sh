#!/bin/bash -e

gradle --info clean build
cp build/libs/tap##1000.war docker
(cd docker && docker build . -t opencadc/alma-tap:1.0.0)

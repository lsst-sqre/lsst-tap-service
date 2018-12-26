#!/bin/bash -ex
# This script monkeypatches the version of cadc-adql
# to the one built locally.  This is because it has
# changes that aren't pushed to master (and the maven
# repos) but they will be.

WORKDIR=`mktemp -d`

cp docker/tap##1000.war $WORKDIR
unzip $WORKDIR/tap##1000.war -d $WORKDIR
rm $WORKDIR/tap##1000.war $WORKDIR/WEB-INF/lib/cadc-adql-*.jar
cp docker/cadc-adql-1.1.2.jar $WORKDIR/WEB-INF/lib
(cd $WORKDIR; zip tap##1000.war -r *)
mv $WORKDIR/tap##1000.war docker/tap##1000.war

rm -rf $WORKDIR

# LSST TAP Service

This repository contains the LSST TAP service.  It is based on the CADC TAP service
code and uses this as a dependency, and then adds special logic to work with QServ.

## Build

Run ./build.sh

## Deployment

### Docker
After the [Build](#build) step above, a set of containers with the `dev` tag will exist
on your local machine.  Then when you run:

`docker-compose up -d && ./waitForContainersReady.sh && ./checkAvailability.sh`

This should start a local group of containers, wait for them to be ready, and then
check that the availability endpoint returns a 200 and a simple sync query works.
This validates that your local TAP implementation is working.  You can now either
use `curl`, TOPCAT, pyvo, or any other local TAP client pointed at
`http://localhost:8080/tap` to test out your TAP service.

Here is an example of using `curl` to query the TAP service:

`curl -L -d 'QUERY=SELECT+TOP+1+*+FROM+TAP_SCHEMA.obscore&LANG=ADQL' http://localhost:8080/tap/sync`

### Pushing to hub.docker.com

After building a set of images (with the `dev` tag), and testing them out, you
can run the `./push.sh` script providing a docker tag to push to.  For example

`./push.sh new_feature_test`

will create a set of containers with the tag `new_feature_test`.  These can
then be used in a k8s environment with the Helm chart located here:

https://github.com/lsst-sqre/charts/tree/master/cadc-tap

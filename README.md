# ALMA TAP

[IVOA TAP](http://ivoa.net/Documents/TAP/) service for the [ALMA Science Archive](http://almascience.nrao.edu/).

## Build

Building the service creates a WAR artifact.  From the root fo the `alma-tap` folder, run:

`gradle --info clean build`

to create the `build/libs/tap##%VERSION%.war` file, where the `%VERSION%` will be replaced with the actual version number declared in the [build.gradle](build.gradle) file.

## Deployment

### Docker
This is a working prototype using a TAP implementation with an Oracle 11 _g_ database.

After the [Build](#build) step above, we can create a Docker deployment like so:

  - `cp build/libs/*.war docker/`
  - `cd docker/`
  - `docker-compose -f docker-compose-withdb.yml up -d && ./waitForContainersReady.sh`

The necessary Docker images will be downloaded, including the large Oracle one, then the service will be available on port `8080`.  You can then issue a request like:

[http://localhost:8080/tap/availability](http://localhost:8080/tap/availability)

Which will provide you with an XML document as to the health of the service.  If it reads with the message:

`The TAP ObsCore service is accepting queries`

Then the TAP service is running properly.  You can then issue a query to the sample ObsCore table:

`curl -L -d 'QUERY=SELECT+TOP+1+*+FROM+TAP_SCHEMA.obscore&LANG=ADQL' http://localhost:8080/tap/sync`

### Dedicated web server

If you have a dedicated Servlet Container (i.e. [Tomcat](http://tomcat.apache.org)) running already, run the [Build](#build) step above, then copy the WAR artifact from `build/libs/` to your Servlet Container's webapp deployment directory.

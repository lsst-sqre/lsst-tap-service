/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2018.                            (c) 2018.
 *  Government of Canada                 Gouvernement du Canada
 *  National Research Council            Conseil national de recherches
 *  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
 *  All rights reserved                  Tous droits réservés
 *
 *  NRC disclaims any warranties,        Le CNRC dénie toute garantie
 *  expressed, implied, or               énoncée, implicite ou légale,
 *  statutory, of any kind with          de quelque nature que ce
 *  respect to the software,             soit, concernant le logiciel,
 *  including without limitation         y compris sans restriction
 *  any warranty of merchantability      toute garantie de valeur
 *  or fitness for a particular          marchande ou de pertinence
 *  purpose. NRC shall not be            pour un usage particulier.
 *  liable in any event for any          Le CNRC ne pourra en aucun cas
 *  damages, whether direct or           être tenu responsable de tout
 *  indirect, special or general,        dommage, direct ou indirect,
 *  consequential or incidental,         particulier ou général,
 *  arising from the use of the          accessoire ou fortuit, résultant
 *  software.  Neither the name          de l'utilisation du logiciel. Ni
 *  of the National Research             le nom du Conseil National de
 *  Council of Canada nor the            Recherches du Canada ni les noms
 *  names of its contributors may        de ses  participants ne peuvent
 *  be used to endorse or promote        être utilisés pour approuver ou
 *  products derived from this           promouvoir les produits dérivés
 *  software without specific prior      de ce logiciel sans autorisation
 *  written permission.                  préalable et particulière
 *                                       par écrit.
 *
 *  This file is part of the             Ce fichier fait partie du projet
 *  OpenCADC project.                    OpenCADC.
 *
 *  OpenCADC is free software:           OpenCADC est un logiciel libre ;
 *  you can redistribute it and/or       vous pouvez le redistribuer ou le
 *  modify it under the terms of         modifier suivant les termes de
 *  the GNU Affero General Public        la “GNU Affero General Public
 *  License as published by the          License” telle que publiée
 *  Free Software Foundation,            par la Free Software Foundation
 *  either version 3 of the              : soit la version 3 de cette
 *  License, or (at your option)         licence, soit (à votre gré)
 *  any later version.                   toute version ultérieure.
 *
 *  OpenCADC is distributed in the       OpenCADC est distribué
 *  hope that it will be useful,         dans l’espoir qu’il vous
 *  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
 *  without even the implied             GARANTIE : sans même la garantie
 *  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
 *  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
 *  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
 *  General Public License for           Générale Publique GNU Affero
 *  more details.                        pour plus de détails.
 *
 *  You should have received             Vous devriez avoir reçu une
 *  a copy of the GNU Affero             copie de la Licence Générale
 *  General Public License along         Publique GNU Affero avec
 *  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
 *  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
 *                                       <http://www.gnu.org/licenses/>.
 *
 *
 ************************************************************************
 */

package org.opencadc.tap.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.Channels;
import java.sql.ResultSet;

import org.apache.solr.s3.S3OutputStream;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import ca.nrc.cadc.dali.tables.TableWriter;
import ca.nrc.cadc.tap.ResultStore;
import ca.nrc.cadc.uws.Job;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

public class ResultStoreImpl implements ResultStore {
    private String filename;
    private static final String bucket = System.getProperty("gcs_bucket");
    private static final String bucketURL = System.getProperty("gcs_bucket_url");
    private static final String bucketType = System.getProperty("gcs_bucket_type");
    private static final String baseURL = System.getProperty("base_url");
    private static final String pathPrefix = System.getProperty("path_prefix");

    @Override
    public URL put(final ResultSet resultSet,
            final TableWriter<ResultSet> resultSetTableWriter)
            throws IOException {
        OutputStream os = getOutputStream();
        resultSetTableWriter.write(resultSet, os);
        os.close();
        return getURL();
    }

    @Override
    public URL put(Throwable throwable, TableWriter tableWriter)
            throws IOException {
        OutputStream os = getOutputStream();
        tableWriter.write(throwable, os);
        os.close();
        return getURL();
    }

    @Override
    public URL put(final ResultSet resultSet,
            final TableWriter<ResultSet> resultSetTableWriter,
            final Integer integer) throws IOException {
        OutputStream os = getOutputStream();

        if (integer == null) {
            resultSetTableWriter.write(resultSet, os);
        } else {
            resultSetTableWriter.write(resultSet, os, integer.longValue());
        }

        os.close();
        return getURL();
    }

    private OutputStream getOutputStream() {
        if (bucketType.equals(new String("S3"))) {
            return getOutputStreamS3();
        } else {
            return getOutputStreamGCS();
        }
    }

    private OutputStream getOutputStreamS3() {
        S3Configuration config = S3Configuration.builder()
                .pathStyleAccessEnabled(true)
                .useArnRegionEnabled(true)
                .build();

        S3Client s3Client = S3Client.builder()
                .endpointOverride(getURI())
                .serviceConfiguration(config)
                .region(Region.US_WEST_2)
                .build();

        return new S3OutputStream(s3Client, filename, bucket);
    }

    private OutputStream getOutputStreamGCS() {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        BlobId blobId = BlobId.of(bucket, filename);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/x-votable+xml").build();
        Blob blob = storage.create(blobInfo);
        return Channels.newOutputStream(blob.writer());
    }

    /**
     * Constructs the URL to the results file.
     * 
     * @return the URL to the results file.
     * @throws MalformedURLException
     */
    private URL getURL() throws MalformedURLException {
        return new URL(baseURL + pathPrefix + "/results/" + filename);
    }

    private URI getURI() {
        try {
            return new URI(bucketURL);
        } catch (URISyntaxException e) {
            // Raise an unchecked exception here to avoid having to change
            // method definitions. This reflects an error in the TAP server
            // configuration and is realistically unrecoverable.
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void setContentType(String contentType) {
    }

    @Override
    public void setJob(Job _job) {
    }

    @Override
    public void setFilename(String filename) {
        this.filename = filename;
    }
}

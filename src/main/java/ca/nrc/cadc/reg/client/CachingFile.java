/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2023.                            (c) 2023.
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
*  $Revision: 5 $
*
************************************************************************
 */

package ca.nrc.cadc.reg.client;

import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.profiler.Profiler;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.UUID;
import org.apache.log4j.Logger;

/**
 * This class will handle the caching of a file based on a remote source.
 * Strategy for caching:
 * <ul>
 * <li>1. Open existing file and check timestamp.
 * <li>2. If not expired, open file and return to user
 * <li>3. Otherwise: read capabilities from web service
 * <li>4. Write new capabilities to temp file
 * <li>5. Atomically move temp file to real file
 * <li>6. Return these refreshed capabilities
 * </ul>
 * 
 * @author majorb
 *
 *
 */
public class CachingFile {

    private static Logger log = Logger.getLogger(CachingFile.class);

    // 10 minutes
    private static final int DEFAULT_EXPRIY_SECONDS = 10 * 60;
    private int connectionTimeout = 3000; // millis
    private int readTimeout = 60000;      // millis

    private File localCache;
    private URL remoteSource;
    private long expirySeconds;
    private File cacheDir;

    /**
     * Construct a caching file with the local file and
     * the remote source. Cache is checked for updates
     * every 10 minutes by default.
     *
     * @param localCache Where to keep the cache
     * @param remoteSource Where to get the content
     */
    public CachingFile(File localCache, URL remoteSource) {
        this(localCache, remoteSource, DEFAULT_EXPRIY_SECONDS);
    }

    /**
     * Construct a caching file with the local file and
     * the remote source.
     *
     * @param localCache Where to keep the cache
     * @param remoteSource Where to get the content
     * @param expirySeconds The number of seconds between cache update checks.
     */
    public CachingFile(File localCache, URL remoteSource, long expirySeconds) {
        if (localCache == null) {
            throw new IllegalArgumentException("localCache param required.");
        }

        this.cacheDir = checkCacheDirectory(localCache);

        if (remoteSource == null) {
            throw new IllegalArgumentException("remoteSource param required.");
        }

        if (remoteSource.getProtocol() == null
                || (!remoteSource.getProtocol().toLowerCase().equals("http")
                && !remoteSource.getProtocol().toLowerCase().equals("https"))) {
            throw new IllegalArgumentException("only http/https schemes allowed in remoteSource");
        }

        this.localCache = localCache;
        this.remoteSource = remoteSource;
        this.expirySeconds = expirySeconds;
    }

    /**
     * HTTP connection timeout in milliseconds (default: 30000).
     * 
     * @param connectionTimeout in milliseconds
     */
    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    /**
     * HTTP read timeout in milliseconds (default: 60000).
     * 
     * @param readTimeout in milliseconds
     */
    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }
    
    private File checkCacheDirectory(File cacheFile) {
        Profiler profiler = new Profiler(CachingFile.class);
        log.debug("Cache file: " + cacheFile);
        try {
            File dir = cacheFile.getParentFile();
            log.debug("cache file parent dir: " + dir);

            if (dir.exists() && !dir.isDirectory()) {
                java.nio.file.Files.delete(dir.toPath());
            }

            if (!dir.exists()) {
                // The NIO version seems to create the path properly,
                // as opposed to the dir.mkdirs()
                // jenkinsd 2017.03.17
                //
                java.nio.file.Files.createDirectories(dir.toPath());
                log.debug("Created directory " + dir);
            }
            return dir;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create directory: " + cacheFile.getParentFile(), e);
        } 
    }

    public String getContent() throws IOException {

        boolean cacheExists = localCache.exists() && localCache.canRead();
        if (cacheExists && !hasExpired()) {
            // read from cached configuration
            log.debug("Reading cache for file " + localCache);
            try {
                return readCache();
            } catch (Exception e) {
                log.warn("Failed to read cached file: " + localCache);
                log.warn("Attempting to read capabilities from source.");
            }
        }

        // read the capabilities from the web service
        try {
            // create a temp file in the directory
            File tmpFile = File.createTempFile(UUID.randomUUID().toString(), null, cacheDir);

            // write the contents to the file
            FileOutputStream fos = new FileOutputStream(tmpFile);
            try {
                loadRemoteContent(fos);
            } catch (IOException e) {
                log.warn("Deleting tmp cache file because download failed.");
                tmpFile.delete();
                throw e;
            } finally {
                try {
                    fos.close();
                } catch (Exception e) {
                    log.warn("Failed to close output stream", e);
                }
            }

            // move the file to the real location
            Path source = Paths.get(tmpFile.getAbsolutePath());
            Path dest = Paths.get(localCache.getAbsolutePath());

            try {
                Files.move(source, dest, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                log.debug("Replaced file " + localCache + " with fresh copy atomically.");
            } catch (AtomicMoveNotSupportedException e) {
                log.warn("Atomic file replacement not supported", e);
                Files.move(source, dest, StandardCopyOption.REPLACE_EXISTING);
                log.debug("Replaced file " + localCache + " with fresh copy (not atomically).");
            }

            return readCache();
        } catch (Exception e) {
            log.warn("Failed to cache capabilities to file: " + localCache, e);
            // for any error return the cached copy if available, otherwise return
            // the capabilities from source
            if (cacheExists) {
                log.warn("Returning expired cached capabilities.");
                return readCache();
            } else {
                log.info("Attemping to return capabilities from source.");
                return getRemoteContent();
            }
        }
    }

    private String readCache() throws IOException {
        // read from cached configuration
        Profiler profiler = new Profiler(CachingFile.class);
        log.debug("Reading cache from " + localCache.getAbsolutePath());
        InputStream in = null;
        ByteArrayOutputStream out = null;
        try {
            in = new FileInputStream(localCache);
            out = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = in.read(buffer)) != -1) {
                out.write(buffer, 0, length);
            }
            return out.toString("UTF-8");
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (Throwable t) {
                    log.warn("failed to close input stream", t);
                }
            }
            if (out != null) {
                try {
                    out.close();
                } catch (Throwable t) {
                    log.warn("failed to close output stream", t);
                }
            }
        }
    }

    private String getRemoteContent() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        loadRemoteContent(out);
        return out.toString("UTF-8");
    }

    private void loadRemoteContent(OutputStream dest) throws IOException {
        Profiler profiler = new Profiler(CachingFile.class);
        try {
            HttpGet download = new HttpGet(remoteSource, dest);
            download.setConnectionTimeout(connectionTimeout);
            download.setReadTimeout(readTimeout);
            download.run();

            if (download.getThrowable() != null) {
                log.warn("Could not get source from " + remoteSource + ": " + download.getThrowable());
                throw new IOException(download.getThrowable());
            }
        } finally {
        }

    }

    private boolean hasExpired() {
        long lastModified = localCache.lastModified();
        long expiryMillis = expirySeconds * 1000;
        long now = System.currentTimeMillis();
        return ((now - lastModified) > expiryMillis);
    }

}

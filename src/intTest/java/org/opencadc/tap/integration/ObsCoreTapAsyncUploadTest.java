package org.opencadc.tap.integration;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.tap.impl.WebTmpUtil;
import ca.nrc.cadc.tap.integration.TapAsyncUploadTest;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;

import javax.security.auth.Subject;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;


/**
 * @author jburke
 */
public class ObsCoreTapAsyncUploadTest extends TapAsyncUploadTest {
    private static final Logger log = Logger.getLogger(ObsCoreTapAsyncUploadTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.tap.impl", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.conformance.uws2", Level.INFO);
        //Log4jInit.setLevel("ca.nrc.cadc.net", Level.DEBUG);
    }

    public ObsCoreTapAsyncUploadTest() {
        super(URI.create("ivo://cadc.nrc.ca/tap"));
        File f = FileUtil.getFileFromResource("TAPUploadTest-1.xml", ObsCoreTapSyncUploadTest.class);
        setTestFile(f);
        setTestURL(getVOTableURL(f));
    }

    static URL getVOTableURL(File f) {
        try {
            String tableName = "tab_" + System.currentTimeMillis();

            // Upload a VOTable to WEBTMP so we have an http url to use
            URL putURL = WebTmpUtil.getURL(tableName, null);
            URL votableUrl = new URL(putURL.toExternalForm().replace("https://", "http://"));

            File cert = WebTmpUtil.getCertFile();
            Subject subject = SSLUtil.createSubject(cert);

            HttpUpload httpUpload = new HttpUpload(f, putURL);
            httpUpload.setContentType("text/xml");
            Subject.doAs(subject, new RunnableAction(httpUpload));

            // Error during the upload, throw an exception.
            if (httpUpload.getThrowable() != null) {
                throw new RuntimeException("setup: failed to store VOTable upload ", httpUpload.getThrowable());
            }

            return votableUrl;
        } catch (MalformedURLException ex) {
            throw new RuntimeException("setup: failed ", ex);
        }
    }
}

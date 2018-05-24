package org.opencadc.tap.integration;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import ca.nrc.cadc.tap.integration.TapSyncUploadTest;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;

import java.io.File;
import java.net.URI;

/**
 * @author jburke
 */
public class ObsCoreTapSyncUploadTest extends TapSyncUploadTest {
    private static final Logger log = Logger.getLogger(ObsCoreTapSyncUploadTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.tap.impl", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.conformance.uws2", Level.INFO);
        //Log4jInit.setLevel("ca.nrc.cadc.net", Level.DEBUG);
    }

    public ObsCoreTapSyncUploadTest() {
        super(URI.create("ivo://cadc.nrc.ca/tap"));
        File f = FileUtil.getFileFromResource("TAPUploadTest-1.xml", ObsCoreTapSyncUploadTest.class);
        setTestFile(f);
        setTestURL(ObsCoreTapAsyncUploadTest.getVOTableURL(f));
    }
}

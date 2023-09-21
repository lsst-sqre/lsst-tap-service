package org.opencadc.tap.impl;

import java.net.MalformedURLException;
import java.net.URL;

import ca.nrc.cadc.dali.util.Format;
import org.apache.log4j.Logger;


public class RubinURLFormat implements Format<Object> {

    private static Logger log = Logger.getLogger(RubinURLFormat.class);

    private static final String BASE_URL = System.getProperty("base_url");

    public RubinURLFormat() {
        super();
    }

    @Override
    public Object parse(String s) {
        throw new UnsupportedOperationException("TAP Formats cannot parse strings.");
    }

    @Override
    public String format(Object o) {
        if (o == null) {
            return "";
        }

	String s = (String) o;

	try {
            URL orig = new URL((String) o);
	    URL base_url = new URL(BASE_URL);
	    URL rewritten = new URL(orig.getProtocol(), base_url.getHost(), orig.getFile());

	    return rewritten.toExternalForm();
	} catch (MalformedURLException ex) {
            throw new RuntimeException("BUG: Failed to rewrite URL: " + s, ex);
        }
    }
}

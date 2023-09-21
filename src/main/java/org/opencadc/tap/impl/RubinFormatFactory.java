package org.opencadc.tap.impl;

import ca.nrc.cadc.dali.util.Format;
import ca.nrc.cadc.tap.TapSelectItem;
import ca.nrc.cadc.tap.writer.format.OracleFormatFactory;
import org.apache.log4j.Logger;

import org.opencadc.tap.impl.RubinURLFormat;


public class RubinFormatFactory extends OracleFormatFactory {

    private static Logger log = Logger.getLogger(RubinFormatFactory.class);

    public RubinFormatFactory() {
    	super();
    }

    @Override
    public Format<Object> getClobFormat(TapSelectItem columnDesc) {
        // function with CLOB argument
        if (columnDesc != null) {
            // ivoa.ObsCore
            if ("ivoa.ObsCore".equalsIgnoreCase(columnDesc.tableName)) {
                if ("access_url".equalsIgnoreCase(columnDesc.getColumnName())) {
	            log.info("getClobFormat called for access_url");
                    return new RubinURLFormat();
                }
            }
        }

        return super.getClobFormat(columnDesc);
    }
}

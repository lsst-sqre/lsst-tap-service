package org.opencadc.tap.impl;

import ca.nrc.cadc.dali.tables.TableWriter;
import ca.nrc.cadc.dali.tables.votable.GroupElement;
import ca.nrc.cadc.dali.tables.votable.ParamElement;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableGroup;
import ca.nrc.cadc.dali.tables.votable.VOTableInfo;
import ca.nrc.cadc.dali.tables.votable.VOTableParam;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.util.Format;
import ca.nrc.cadc.dali.util.FormatFactory;
import ca.nrc.cadc.xml.ContentConverter;
import ca.nrc.cadc.xml.IterableContent;
import ca.nrc.cadc.xml.MaxIterations;
import uk.ac.starlink.table.ColumnInfo;
import uk.ac.starlink.table.DescribedValue;
import uk.ac.starlink.table.RowSequence;
import uk.ac.starlink.table.WrapperRowSequence;
import uk.ac.starlink.table.jdbc.SequentialResultSetStarTable;
import uk.ac.starlink.votable.DataFormat;
import uk.ac.starlink.votable.VOSerializer;
import uk.ac.starlink.votable.VOTableVersion;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.output.XMLOutputter;

/**
 * ResultSet to VOTable writer.
 *
 * @author stvoutsin
 */
public class ResultSetWriter implements TableWriter<ResultSet> {

    private static final Logger log = Logger.getLogger(ResultSetWriter.class);
    private static final String BASE_URL = System.getProperty("base_url");

    public static final String CONTENT_TYPE = "application/x-votable+xml";
    public static final String CONTENT_TYPE_ALT = "text/xml";

    // VOTable Version number.
    public static final String VOTABLE_VERSION = "1.3";

    // Uri to the XML schema.
    public static final String XSI_SCHEMA = "http://www.w3.org/2001/XMLSchema-instance";

    // Uri to the VOTable schema.
    public static final String VOTABLE_11_NS_URI = "http://www.ivoa.net/xml/VOTable/v1.1";
    public static final String VOTABLE_12_NS_URI = "http://www.ivoa.net/xml/VOTable/v1.2";
    public static final String VOTABLE_13_NS_URI = "http://www.ivoa.net/xml/VOTable/v1.3";
    public static final String VOTABLE_14_NS_URI = "http://www.ivoa.net/xml/VOTable/v1.4";

    private FormatFactory formatFactory;
    private String mimeType;
    private DataFormat dfmt_;
    private VOTableVersion version_;
    private long maxrec_;
    private List<VOTableInfo> infos;
    private List<VOTableResource> resources;
    private ColumnInfo[] columns;
    private long totalRows = 0;

    /**
     * Get the total number of rows processed.
     *
     * @return the number of rows written
     */
    public long getRowCount() {
        return totalRows;
    }
    
    /**
     * Default constructor.
     */
    public ResultSetWriter() {
        this(null, uk.ac.starlink.votable.DataFormat.BINARY2, VOTableVersion.V13, -1);
    }


    /**
     * Constructor.
     *
     * @param  mimeType selects the mimetype string
     * @param  dfmt  selects VOTable serialization format
     *               (TABLEDATA, BINARY, BINARY2, FITS)
     * @param  version  selects VOTable version
     * @param  maxrec   maximum record count before overflow;
     *                  negative value means no limit
     */
    public ResultSetWriter( String mimeType, DataFormat dfmt, VOTableVersion version, long maxrec ) {
    	this.mimeType = mimeType;
    	this.dfmt_ = dfmt;
    	this.version_ = version;
    	this.maxrec_ = maxrec;
        this.infos = new ArrayList<>();
        this.resources = new ArrayList<>();
        this.columns = null;
    }

    
    /**
     * Get the Content-Type for the VOTable.
     *
     * @return VOTable Content-Type.
     */
    @Override
    public String getContentType() {
        if (mimeType == null) {
            return CONTENT_TYPE;
        }

        return mimeType;
    }

    public List<VOTableResource> getResources() {
        return resources;
    }

    /**
     * Get the list of VOTable infos.
     *
     * @return List of VOTableInfo objects
     */
    public List<VOTableInfo> getInfos() {
        return infos;
    }
    
    public ColumnInfo[] getColumns() {
        return columns;
    }
    
    /**
     * Set the list of VOTable infos.
     *
     * @param infos List of VOTableInfo objects
     */
    public void setInfos(List<VOTableInfo> infos) {
        this.infos = infos;
    }
    
    public void setColumns(ColumnInfo[] columns) {
        this.columns = columns;
    }
    
    
    /**
     * Set the resources of the VOTable.
     *
     * @param resources List of VOTableResource objects
     */
    public void setResources(List<VOTableResource> resources) {
        this.resources = resources;
    }
    
    /**
     * Add a single VOTable info.
     *
     * @param info VOTableInfo object to add
     */
    public void addInfo(VOTableInfo info) {
        if (info != null) {
            this.infos.add(info);
        }
    }

    
    /**
     * Add a single VOTableResource object.
     *
     * @param resource VOTableResource object to add
     */
    public void addResource(VOTableResource resource) {
        if (resource != null) {
            this.resources.add(resource);
        }
    }  
    /**
     * Get error content type
     */
    public String getErrorContentType() {
        return getContentType();
    }

    /**
     * Get the extension for the VOTable.
     *
     * @return VOTable extension.
     */
    @Override
    public String getExtension() {
        return "xml";
    }

    @Override
    public void setFormatFactory(FormatFactory formatFactory) {
        this.formatFactory = formatFactory;
    }

    /**
     * Write the ResultSet to the specified OutputStream.
     *
     * @param resultSet ResultSet object to write.
     * @param ostream OutputStream to write to.
     * @throws IOException if problem writing to OutputStream.
     */
    @Override
    public void write(ResultSet resultSet, OutputStream ostream)
            throws IOException {
        write(resultSet, ostream, Long.MAX_VALUE);
    }

    /**
     * Write the ResultSet to the specified OutputStream, only writing maxrec rows.
     * If the VOTable contains more than maxrec rows, appends an INFO element with
     * name="QUERY_STATUS" value="OVERFLOW" to the VOTable.
     *
     * @param resultSet ResultSet object to write.
     * @param ostream OutputStream to write to.
     * @param maxrec maximum number of rows to write.
     * @throws IOException if problem writing to OutputStream.
     */
    @Override
    public void write(ResultSet resultSet, OutputStream ostream, Long maxrec)
            throws IOException {
        Writer writer = new BufferedWriter(new OutputStreamWriter(ostream, "UTF-8"));
        write(resultSet, writer, maxrec);
    }
    
    /**
     * Write the ResultSet to the specified Writer.
     *
     * @param resultSet ResultSet object to write.
     * @param writer Writer to write to.
     * @throws IOException if problem writing to the writer.
     */
    @Override
    public void write(ResultSet resultSet, Writer writer)
            throws IOException {
        write(resultSet, writer, Long.MAX_VALUE);
    }

    /**
     * Write the ResultSet to the specified Writer, only writing maxrec rows.
     * If the ResultSet contains more than maxrec rows, appends an INFO element with
     * name="QUERY_STATUS" value="OVERFLOW" to the VOTable.
     *
     * @param resultSet ResultSet object to write.
     * @param writer Writer to write to.
     * @param maxrec maximum number of rows to write.
     * @throws IOException if problem writing to the writer.
     */
    @Override
    public void write(ResultSet resultSet, Writer writer, Long maxrec)
            throws IOException {
        if (formatFactory == null) {
            this.formatFactory = new FormatFactory();
        }
        writeImpl(resultSet, writer, maxrec);
    }
    
    /**
     * Write the Throwable to a VOTable, creating an INFO element with
     * name="QUERY_STATUS" value="ERROR" and setting the stacktrace as
     * the INFO text.
     *
     * @param thrown Throwable to write.
     * @param output OutputStream to write to.
     * @throws IOException if problem writing to the stream.
     */
    public void write(Throwable thrown, OutputStream output)
            throws IOException {
        Document document = createDocument();
        Element root = document.getRootElement();
        Namespace namespace = root.getNamespace();

        // Create the RESOURCE element and add to the VOTABLE element.
        Element resource = new Element("RESOURCE", namespace);
        resource.setAttribute("type", "results");
        root.addContent(resource);

        // Create the INFO element and add to the RESOURCE element.
        Element info = new Element("INFO", namespace);
        info.setAttribute("name", "QUERY_STATUS");
        info.setAttribute("value", "ERROR");
        info.setText(getThrownExceptions(thrown));
        resource.addContent(info);

        // Write out the VOTABLE.
        XMLOutputter outputter = new XMLOutputter();
        outputter.setFormat(org.jdom2.output.Format.getPrettyFormat());
        outputter.output(document, output);
    }
    
    /**
     * Writes a result set to an output stream as a VOTable.
     *
     * @param   resultSet  ResultSet
     * @param   writer  Writer
     * @param   maxrec Maximum number of rows
     */
    protected void writeImpl(ResultSet resultSet, Writer writer, Long maxrec)
            throws IOException {
        log.debug("write, maxrec=" + maxrec);

        
        try (BufferedWriter out = (writer instanceof BufferedWriter)
                ? (BufferedWriter) writer
                : new BufferedWriter(writer)) {

        	
            if (maxrec != null && maxrec == 0 || resultSet == null) {
                writeEmptyResult(out);
                return;
            }
        	
            LimitedResultSetStarTable table;
            try {
                table = new LimitedResultSetStarTable(this.getColumns(), resultSet, maxrec);
            } catch (SQLException e) {
                throw new IOException("Error processing ResultSet: " + e.getMessage(), e);
            }
	
	        /* Prepares the object that will do the serialization work. */
	        VOSerializer voser =
	            VOSerializer.makeSerializer( dfmt_, version_, table );

	        /* Write header. */
	        out.write( "<VOTABLE"
	                 + VOSerializer.formatAttribute( "version",
	                                                 version_.getVersionNumber() )
	                 + VOSerializer.formatAttribute( "xmlns",
	                                                 version_.getXmlNamespace() )
	                 + ">" );
	        out.newLine();
	        out.write( "<RESOURCE type='results'>" );
	        out.newLine();
	        
            XMLOutputter outputter = new XMLOutputter();
            outputter.setFormat(org.jdom2.output.Format.getPrettyFormat());
            
	        // Write all info elements
	        for (VOTableInfo info : infos) {
	        	Element infoElement = new Element("INFO");
	            infoElement.setAttribute("name", info.getName());
	            infoElement.setAttribute("value", info.getValue());
	            outputter.output(infoElement, out);
	            out.newLine();
	        }
	        
	        /* Write table element. */
	        voser.writeInlineTableElement( out );
	
	        /* Check for overflow and write INFO if required. */
	        if ( table.lastSequenceOverflowed() ) {
	            out.write( "<INFO name='QUERY_STATUS' value='OVERFLOW'/>" );
	            out.newLine();
	        }

	        /* Write footer. */
	        out.write( "</RESOURCE>" );
	        out.newLine();
	        for (VOTableResource resource : resources) {
	        	Element r = createResource(resource, null);
	            outputter.output(r, out);
	            out.newLine();
	        }
	        out.write( "</VOTABLE>" );
	        out.newLine();
	        out.flush();
	        
	        this.totalRows = table.getTotalRows();
	
		}
    }
    
    /**
     * Write out an empty result (Used when maxrec=0)
     * 
     * @param out
     */
    void writeEmptyResult(BufferedWriter out) {
	    try {
	        /* Write header. */
	        out.write("<VOTABLE"
	                + VOSerializer.formatAttribute("version", version_.getVersionNumber())
	                + VOSerializer.formatAttribute("xmlns", version_.getXmlNamespace())
	                + ">");
	        out.newLine();
	        out.write("<RESOURCE type='results'>");
	        out.newLine();

	        XMLOutputter outputter = new XMLOutputter();
	        outputter.setFormat(org.jdom2.output.Format.getPrettyFormat());

	        // Write all info elements
	        for (VOTableInfo info : infos) {
	            Element infoElement = new Element("INFO");
	            infoElement.setAttribute("name", info.getName());
	            infoElement.setAttribute("value", info.getValue());
	            outputter.output(infoElement, out);
	            out.newLine();
	        }

	        // Add TABLE element and column metadata
	        out.write("<TABLE>\n");
	        
	        // Write all column metadata
	        for (ColumnInfo colInfo : columns) {
	            Element field = new Element("FIELD");
	            field.setAttribute("name", colInfo.getName());
	            
	            // Set datatype
	            if (colInfo.getContentClass() != null) {
	                String datatype = getVOTableDatatype(colInfo.getContentClass());
	                if (datatype != null) {
	                    field.setAttribute("datatype", datatype);
	                    
	                    // Handle array types
	                    if (colInfo.getContentClass().isArray()) {
	                        if (colInfo.getContentClass().getComponentType() == String.class) {
	                            field.setAttribute("arraysize", "*");
	                        } else if (colInfo.getShape() != null && colInfo.getShape().length > 0) {
	                            field.setAttribute("arraysize", String.valueOf(colInfo.getShape()[0]));
	                        }
	                    }
	                }
	            }
	            
	            // Add standard metadata
	            if (colInfo.getUnitString() != null) {
	                field.setAttribute("unit", colInfo.getUnitString());
	            }
	            if (colInfo.getUCD() != null) {
	                field.setAttribute("ucd", colInfo.getUCD());
	            }
	            if (colInfo.getUtype() != null) {
	                field.setAttribute("utype", colInfo.getUtype());
	            }
	            
	            // Add ID if present
	            DescribedValue idValue = colInfo.getAuxDatumByName("ID_INFO");
	            if (idValue != null && idValue.getValue() != null) {
	                field.setAttribute("ID", idValue.getValue().toString());
	            }
	            
	            // Add description if present
	            String description = colInfo.getDescription();
	            if (description != null && !description.isEmpty()) {
	                Element desc = new Element("DESCRIPTION");
	                desc.setText(description);
	                field.addContent(desc);
	            }
	            
	            outputter.output(field, out);
	            out.newLine();
	        }

	        // Write the empty data section
	        out.write("<DATA>\n");
	        out.write("<BINARY2>\n");
	        out.write("<STREAM encoding='base64'>\n");
	        out.write("</STREAM>\n");
	        out.write("</BINARY2>\n");
	        out.write("</DATA>\n");
	        
	        out.write("</TABLE>\n");  // Close the TABLE element

	        /* Write footer. */
	        out.write("</RESOURCE>");
	        out.newLine();
	        for (VOTableResource resource : resources) {
	            Element r = createResource(resource, null);
	            outputter.output(r, out);
	            out.newLine();
	        }
	        out.write("</VOTABLE>");
	        out.newLine();
	        out.flush();

	    } catch (IOException e) {
	        e.printStackTrace();
	    }

	    this.totalRows = 0;
    }

    /**
     * Convert Java class to VOTable datatype
     * 
     * @param clazz
     * @return
     */
	private String getVOTableDatatype(Class<?> clazz) {
	    if (clazz == Boolean.class || clazz == boolean.class) return "boolean";
	    if (clazz == Byte.class || clazz == byte.class) return "unsignedByte";
	    if (clazz == Short.class || clazz == short.class) return "short";
	    if (clazz == Integer.class || clazz == int.class) return "int";
	    if (clazz == Long.class || clazz == long.class) return "long";
	    if (clazz == Float.class || clazz == float.class) return "float";
	    if (clazz == Double.class || clazz == double.class) return "double";
	    if (clazz == Character.class || clazz == char.class) return "char";
	    if (clazz == String.class) return "char";
	    return null;
	}

    /**
     * StarTable implementation which is based on a ResultSet, and which
     * is limited to a fixed number of rows when its row iterator is used.
     * Note this implementation is OK for one-pass table output handlers
     * like VOTable, but won't work for ones which require two passes
     * such as FITS (which needs row count up front).
     */
    private static class LimitedResultSetStarTable
            extends SequentialResultSetStarTable {
    	
        public static final String TABLE_NAME_INFO = "TABLE_NAME";
        public static final String ACTUAL_COLUMN_NAME_INFO = "COLUMN_NAME";
        private final long maxrec_;
        private boolean overflow_;
        private long totalRows = 0;
		private ColumnInfo[] columnInfos;

        /**
         * Constructor.
         *
         * @param   rset  result set supplying the data
         * @param   maxrec   maximum number of rows that will be iterated over;
         *                   negative value means no limit
         */
        LimitedResultSetStarTable(ColumnInfo[] colInfos, ResultSet rset, long maxrec )
                throws SQLException {
            super( rset );
            maxrec_ = maxrec;
			columnInfos = colInfos;
        }
        
        /**
         * Get the row count 
         */
        public long getTotalRows() {
           return totalRows;
        }

        /**
         * Provides column information for a specified index.
         *
         * @param  colInd  index of the column to describe
         * @return  metadata object describing this column
         */
        @Override
        public ColumnInfo getColumnInfo(final int colInd) {
            return columnInfos[colInd];
        }
            
        /**
         * Indicates whether the last row sequence dispensed by
         * this table's getRowSequence method was truncated at maxrec rows.
         *
         * @return   true iff the last row sequence overflowed
         */
        public boolean lastSequenceOverflowed() {
            return overflow_;
        }
        
        /**
         * A RowSequence wrapper that modifies obscore access URLs in result sets.
         * This class intercepts access_url column values and rewrites them to use a different base URL
         *
         * @throws RuntimeException if URL rewriting fails due to malformed URLs
         */
        private static class ModifiedLimitRowSequence extends WrapperRowSequence {
            private final Map<Integer, Boolean> accessUrlColumns;
    		private ColumnInfo[] columnInfos;
    		
            ModifiedLimitRowSequence(RowSequence baseSeq, ColumnInfo[] columnInfos) {
                super(baseSeq);
                this.accessUrlColumns = findAccessUrlColumns(columnInfos);
                this.columnInfos = columnInfos;
            }
            
            /**
             * Find all access_url columns and determine if they belong to ivoa.ObsCore.
             * Returns a map of column indices to boolean indicating if they should be modified.
             */
            private static Map<Integer, Boolean> findAccessUrlColumns(ColumnInfo[] columnInfos) {
                Map<Integer, Boolean> columns = new HashMap<>();
                try {
	                for (int i = 0; i < columnInfos.length; i++) {
	                    ColumnInfo info = columnInfos[i];
	                    
	                    DescribedValue tableNameValue = info.getAuxDatumByName(TABLE_NAME_INFO);
	                    DescribedValue actualColumnValue =  info.getAuxDatumByName(ACTUAL_COLUMN_NAME_INFO);
	                    if (tableNameValue != null) {
	                        String tableName = tableNameValue.getValue().toString();
	                        String actualColName = actualColumnValue.getValue().toString();
	                        
	                        if ("access_url".equals(actualColName)) {
	                            boolean isObsCore = "ivoa.ObsCore".equals(tableName);
	                            columns.put(i, isObsCore);
	                        }
	                    }
	                }
                } catch (NullPointerException ex) {
                	log.debug("Error while trying to find access_url columns");
                }

                return columns;
            }
            
            /**
             * Return values from a row, applying necessary type conversions and URL modifications.
             *
             * This method handles two types of transformations:
             * 1. String to Character conversion for VOTable char datatype fields
             * 2. URL rewriting for access_url columns in ObsCore tables
             *
             * For char datatype columns: 
             * - If the input is a string and the column expects char, take the first character
             * - Empty strings are converted to null
             * - Non-string values are passed through unchanged
             * 
             * @return array containing values for the current row, with appropriate type conversions
             * @throws IOException if there is an error reading from the underlying sequence
             */
            @Override
            public Object[] getRow() throws IOException {
                Object[] row = super.getRow();
                if (row == null) {
                    return null;
                }
                
                for (int i = 0; i < row.length; i++) {
                    Object value = row[i];
                    ColumnInfo info = columnInfos[i];
                    
                    if (value != null) {
                        
                        // Handle conversion from String to Character for char columns
                        if (value instanceof String && info.getContentClass() == Character.class) {
                            String strVal = (String)value;
                            if (strVal.length() > 0) {
                                row[i] = strVal.charAt(0);
                            } else {
                                row[i] = null;
                            }
                        }
                    }
                    
                    if (shouldModifyUrl(i, row[i])) {
                        row[i] = modifyAccessUrl((String)row[i]);
                    }
                }
                
                return row;
            }
            
            /**
             * Returns the value from a single cell, applying necessary type conversions and URL modifications.
             *
             * @param  icol  the column index
             * @return  the cell contents after any necessary conversions
             * @throws IOException if there is an error reading from the underlying sequence
             */
            @Override
            public Object getCell(int icol) throws IOException {
                Object value = super.getCell(icol);
                ColumnInfo info = columnInfos[icol];
                
                if (value != null) {
                    // Handle conversion from String to Character for char columns
                    if (value instanceof String && info.getContentClass() == Character.class) {
                        String strVal = (String)value;
                        if (strVal.length() > 0) {
                            value = strVal.charAt(0);
                        } else {
                            value = null;
                        }
                    }
                }
                
                if (shouldModifyUrl(icol, value)) {
                    value = modifyAccessUrl((String)value);
                }
                
                return value;
            }
                
            /**
             * Determines whether a value at a given index should have its URL modified.
             *
             * @param  columnIndex  index of the column containing the value
             * @param  value  the value to check
             * @return  true if the value should have its URL modified
             */
            private boolean shouldModifyUrl(int columnIndex, Object value) {
                return value instanceof String && 
                       accessUrlColumns.containsKey(columnIndex) && 
                       accessUrlColumns.get(columnIndex) && 
                       value != null;
            }

            /**
             * Modifies a URL to use the base URL while preserving the path.
             *
             * @param  url  the original URL to modify, or null
             * @return  the modified URL, or null if the input was null
             * @throws RuntimeException if URL parsing or rewriting fails
             */
            private String modifyAccessUrl(String url) {
                if (url != null) {
	            	String s = (String) url;
	            	try {
	                    URL orig = new URL(s);
	            	    URL base_url = new URL(BASE_URL);
	            	    URL rewritten = new URL(orig.getProtocol(), base_url.getHost(), orig.getFile());
	            	    log.debug( "Rewritten URL: " + rewritten.toExternalForm());
	            	    return rewritten.toExternalForm();
	            	} catch (MalformedURLException ex) {
	                    throw new RuntimeException("BUG: Failed to rewrite URL: " + s, ex);
	                }
	            }
                return url;
            }
        }
        
        /**
         * Returns a row sequence that may be limited to a maximum number of rows.
         * Creates a sequence that wraps the underlying ResultSet, applies URL modifications
         * if needed, and enforces any row limit specified by maxrec_.
         *
         * @return  a row sequence, possibly truncated at maxrec_ rows
         * @throws IOException if there is an error reading from the base sequence
         */
        @Override
        public RowSequence getRowSequence() throws IOException {
            overflow_ = false;
            totalRows = 0;
            RowSequence baseSeq = super.getRowSequence();
            baseSeq = new ModifiedLimitRowSequence(baseSeq, columnInfos);
            
            if (maxrec_ < 0 || maxrec_ == Long.MAX_VALUE) {
                return new WrapperRowSequence(baseSeq) {
                    @Override
                    public boolean next() throws IOException {
                        boolean hasNext = super.next();
                        if (hasNext) {
                            totalRows++;
                        }
                        return hasNext;
                    }
                };            
            } else {
                return new WrapperRowSequence( baseSeq ) {
                    long irow = -1;
                    @Override
                    public boolean next() throws IOException {
                        irow++;
                        if ( irow < maxrec_ ) {
                            boolean hasNext = super.next();
                            if (hasNext) {
                                totalRows++;
                            }
                            return hasNext;
                        }
                        if ( irow == maxrec_ ) {
                        	log.debug("Overflow reached while processing rows!");
                            overflow_ = super.next();
                        }
                        return false;
                    }
                };
            }
        }
    }
    
    /**
     * Creates a JDOM Element representing a VOTable RESOURCE from a VOTableResource object.
     * Converts the VOTableResource into XML format, including all its attributes, description,
     * INFO elements, parameters, and groups.
     *
     * @param  votResource  the VOTableResource to convert
     * @param  namespace   the XML namespace to use, may be null
     * @return  JDOM Element containing the XML representation of the resource
     */
    private Element createResource(VOTableResource votResource, Namespace namespace) {
        // Create the RESOURCE element and add to the VOTABLE element.
        Element resource = new Element("RESOURCE", namespace);

        resource.setAttribute("type", votResource.getType());
        log.debug("wrote resource.type: " + votResource.getType());

        if (votResource.id != null) {
            resource.setAttribute("ID", votResource.id);
        }

        if (votResource.getName() != null) {
            resource.setAttribute("name", votResource.getName());
        }

        if (votResource.utype != null) {
            resource.setAttribute("utype", votResource.utype);
        }

        // Create the DESCRIPTION element and add to RESOURCE element.
        if (votResource.description != null) {
            Element description = new Element("DESCRIPTION", namespace);
            description.setText(votResource.description);
            resource.addContent(description);
        }

        // Create the INFO element and add to the RESOURCE element.
        for (VOTableInfo in : votResource.getInfos()) {
            Element info = new Element("INFO", namespace);
            info.setAttribute("name", in.getName());
            info.setAttribute("value", in.getValue());
            if (in.content != null) {
                info.setText(in.content);
            }
            resource.addContent(info);
        }
        log.debug("wrote resource.info: " + votResource.getInfos().size());

        for (VOTableParam param : votResource.getParams()) {
            resource.addContent(new ParamElement(param, namespace));
        }
        log.debug("wrote resource.param: " + votResource.getParams().size());

        for (VOTableGroup vg : votResource.getGroups()) {
            resource.addContent(new GroupElement(vg, namespace));
        }

        return resource;
    }
    
    /**
     * Builds a empty VOTable document with the appropriate namespaces and
     * attributes.
     *
     * @return VOTable document.
     */
    protected Document createDocument() {
        // the root VOTABLE element
        Namespace vot = Namespace.getNamespace(VOTABLE_13_NS_URI);
        Namespace xsi = Namespace.getNamespace("xsi", XSI_SCHEMA);
        Element votable = new Element("VOTABLE", vot);
        votable.setAttribute("version", VOTABLE_VERSION);
        votable.addNamespaceDeclaration(xsi);

        Document document = new Document();
        document.addContent(votable);

        return document;
    }

    /**
     * Builds a string containing all exception messages from a throwable and its causes.
     *
     * @param  thrown  the throwable to process
     * @return  space-separated string of all exception messages in the chain
     */
    private String getThrownExceptions(Throwable thrown) {
        StringBuilder sb = new StringBuilder();
        if (thrown.getMessage() == null) {
            sb.append("");
        } else {
            sb.append(thrown.getMessage());
        }
        while (thrown.getCause() != null) {
            thrown = thrown.getCause();
            sb.append(" ");
            if (thrown.getMessage() == null) {
                sb.append("");
            } else {
                sb.append(thrown.getMessage());
            }
        }
        String result = sb.toString().trim();
        return result.isEmpty() ? "An unknown error occurred" : result;
        
    }


}

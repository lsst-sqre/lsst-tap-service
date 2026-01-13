/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2009.                            (c) 2009.
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
*  $Revision: 4 $
*
************************************************************************
*/

package org.opencadc.tap.impl;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.dali.tables.ascii.AsciiTableWriter;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableInfo;
import ca.nrc.cadc.dali.tables.votable.VOTableParam;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;

import org.opencadc.tap.impl.ResultSetWriter;
import ca.nrc.cadc.dali.util.Format;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.tap.TapSelectItem;
import ca.nrc.cadc.tap.TableWriter;
import ca.nrc.cadc.tap.schema.TapDataType;
import ca.nrc.cadc.tap.writer.ResultSetTableData;
import ca.nrc.cadc.tap.writer.RssTableWriter;
import ca.nrc.cadc.tap.writer.format.FormatFactory;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.ParameterUtil;
import uk.ac.starlink.table.ColumnInfo;
import uk.ac.starlink.table.DefaultValueInfo;
import uk.ac.starlink.table.DescribedValue;
import uk.ac.starlink.votable.VOStarTable;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.security.auth.Subject;
import org.stringtemplate.v4.ST;
import org.apache.log4j.Logger;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class RubinTableWriter implements TableWriter
{

    private static final Logger log = Logger.getLogger(RubinTableWriter.class);
    public static final String TABLE_NAME_INFO = "TABLE_NAME";
    public static final String ACTUAL_COLUMN_NAME_INFO = "COLUMN_NAME";

    private static final String FORMAT = "RESPONSEFORMAT";
    private static final String FORMAT_ALT = "FORMAT";

    // shortcuts
    public static final String CSV = "csv";
    public static final String FITS = "fits";
    public static final String HTML = "html";
    public static final String TEXT = "text";
    public static final String TSV = "tsv";
    public static final String VOTABLE = "votable";
    public static final String VOTABLE_TD = "votable-td";
    public static final String RSS = "rss";

    // content-type
//    private static final String APPLICATION_FITS = "application/fits";
    private static final String APPLICATION_VOTABLE_XML = "application/x-votable+xml";
    private static final String APPLICATION_VOTABLE_B2 = "application/x-votable+xml;serialization=binary2";
    private static final String APPLICATION_VOTABLE_TD_XML = "application/x-votable+xml;serialization=tabledata";
    private static final String APPLICATION_RSS = "application/rss+xml";
    private static final String TEXT_XML_VOTABLE = "text/xml;content=x-votable"; // the SIAv1 mimetype
    private static final String TEXT_CSV = "text/csv";
//    private static final String TEXT_HTML = "text/html";
//    private static final String TEXT_PLAIN = "text/plain";
    private static final String TEXT_TAB_SEPARATED_VALUES = "text/tab-separated-values";
    private static final String TEXT_XML = "text/xml";

    private static final Map<String,String> knownFormats = new TreeMap<String,String>();

    private static final String baseUrl = System.getProperty("base_url");
    private static final String datalinkConfig = "/tmp/datalink/";

    static
    {
        knownFormats.put(APPLICATION_VOTABLE_XML, VOTABLE);
        knownFormats.put(APPLICATION_VOTABLE_B2, VOTABLE);
        knownFormats.put(APPLICATION_VOTABLE_TD_XML, VOTABLE_TD);
        knownFormats.put(TEXT_XML, VOTABLE);
        knownFormats.put(TEXT_XML_VOTABLE, VOTABLE);
        knownFormats.put(TEXT_CSV, CSV);
        knownFormats.put(TEXT_TAB_SEPARATED_VALUES, TSV);
        knownFormats.put(VOTABLE, VOTABLE);
        knownFormats.put(CSV, CSV);
        knownFormats.put(TSV, TSV);
        knownFormats.put(RSS, RSS);
        knownFormats.put(APPLICATION_RSS, RSS);
    }

    private Job job;
    private String queryInfo;
    private String contentType;
    private String extension;

    // RssTableWriter not yet ported to cadcDALI
    private RssTableWriter rssTableWriter;
    private ca.nrc.cadc.dali.tables.TableWriter<ResultSet> resultSetWriter;
    private ca.nrc.cadc.dali.tables.TableWriter<VOTableDocument> voDocumentWriter;
    
    private FormatFactory formatFactory;
    private boolean errorWriter = false;
    
    private long rowcount = 0l;

    // once the RssTableWriter is converted to use the DALI format
    // of writing, this reference will not be needed
    List<TapSelectItem> selectList;

    public RubinTableWriter() { 
        this(false); 
    }

    public RubinTableWriter(boolean errorWriter) {
        this.errorWriter = errorWriter;
    }

    @Override
    public void setJob(Job job)
    {
        this.job = job;
        initFormat();
    }

    @Override
    public void setSelectList(List<TapSelectItem> selectList)
    {
        this.selectList = selectList;
        if (rssTableWriter != null)
            rssTableWriter.setSelectList(selectList);
    }
    
    @Override
    public void setQueryInfo(String queryInfo)
    {
        this.queryInfo = queryInfo;
    }

    @Override
    public String getContentType()
    {
    	String contentType = null;
    	
    	if (resultSetWriter != null) {
    		contentType = resultSetWriter.getContentType();
    	} else if (resultSetWriter != null) {
    		contentType = resultSetWriter.getContentType();
    	}
    	
		return contentType;
    }

    @Override
    public String getErrorContentType()
    {
        return resultSetWriter.getErrorContentType();
    }

    /**
     * Get the number of rows the output table
     * 
     * @return number of result rows written in output table
     */
    public long getRowCount()
    {
        return rowcount;
    }
    
    public String getExtension()
    {
        return extension;
    }
    
    private void initFormat()
    {
        String format = ParameterUtil.findParameterValue(FORMAT, job.getParameterList());
        if (format == null)
            format = ParameterUtil.findParameterValue(FORMAT_ALT, job.getParameterList());
        if (format == null)
            format = VOTABLE;
        
        String type = knownFormats.get(format.toLowerCase());
        if (type == null && errorWriter) {
            type = VOTABLE;
            format = VOTABLE;
        } else if (type == null) {
            throw new UnsupportedOperationException("unknown format: " + format);
        }
        
        if (type.equals(VOTABLE) && format.equals(VOTABLE))
            format = APPLICATION_VOTABLE_XML;
        
        // Create the table writer (handle RSS the old way for now)
        // Note: This needs to be done before the write method is called so the contentType
        // can be determined from the table writer.

        switch (type) {
	        case RSS:
	            rssTableWriter = new RssTableWriter();
	            rssTableWriter.setJob(job);
	            // For error handling
	            voDocumentWriter = new AsciiTableWriter(AsciiTableWriter.ContentType.TSV);
	            break;
	
	        case VOTABLE:
	            resultSetWriter = new ResultSetWriter();
	            this.contentType = resultSetWriter.getContentType();
	            this.extension = resultSetWriter.getExtension();
	            break;
	            
	        case VOTABLE_TD:
	            voDocumentWriter = new VOTableWriter();
	            this.contentType = voDocumentWriter.getContentType();
	            this.extension = voDocumentWriter.getExtension();
	            break;
	            
	        case CSV:
	            voDocumentWriter = new AsciiTableWriter(AsciiTableWriter.ContentType.CSV);
	            break;
	
	        case TSV:
	            voDocumentWriter = new AsciiTableWriter(AsciiTableWriter.ContentType.TSV);
	            break;
	
	        default:
	            throw new UnsupportedOperationException("unsupported format: " + type);
		}
		
	    if (voDocumentWriter != null) {
	        this.contentType = voDocumentWriter.getContentType();
	        this.extension = voDocumentWriter.getExtension();
	    }


    }

    public void setFormatFactory(FormatFactory formatFactory)
    {
        this.formatFactory = formatFactory;
    }

    @Override
    public void setFormatFactory(ca.nrc.cadc.dali.util.FormatFactory formatFactory)
    {
        throw new UnsupportedOperationException("Use custom tap format factory implementation class");
    }

    public void write(Throwable t, OutputStream out) 
        throws IOException
    {
    	if (this.resultSetWriter != null) {
    		this.resultSetWriter.write(t, out);
    	} else if (this.voDocumentWriter != null) {
    		this.voDocumentWriter.write(t, out);	
    	}
    }

    
    @Override
    public void write(ResultSet rs, OutputStream out) throws IOException
    {
        this.write(rs, out, null);
    }

    public void write(ResultSet rs, OutputStream out, Long maxrec) throws IOException
    {
        if (rs != null && log.isDebugEnabled())
            try { log.debug("resultSet column count: " + rs.getMetaData().getColumnCount()); }
            catch(Exception oops) { log.error("failed to check resultset column count", oops); }

        if (rssTableWriter != null)
        {
            rssTableWriter.setJob(job);
            rssTableWriter.setSelectList(selectList);
            rssTableWriter.setFormatFactory(formatFactory);
            rssTableWriter.setQueryInfo(queryInfo);
            if (maxrec != null)
                rssTableWriter.write(rs, out, maxrec);
            else
                rssTableWriter.write(rs, out);
            return;
        }

        VOTableDocument votableDocument = new VOTableDocument();

        VOTableResource resultsResource = new VOTableResource("results");
        VOTableTable resultsTable = new VOTableTable();

        // get the formats based on the selectList
        List<Format<Object>> formats = formatFactory.getFormats(selectList);
        List<String> columnNames = new ArrayList<String>();

        int listIndex = 0;
        
        List<ColumnInfo> columnInfoList = new ArrayList<>();

        // Add the metadata elements.
        for (TapSelectItem resultCol : selectList)
        {
            VOTableField newField = createVOTableField(resultCol);
            newField.id = "col_" + listIndex;
            resultsTable.getFields().add(newField);

            String fullColumnName = resultCol.tableName + "_" + resultCol.getColumnName();
            columnNames.add(fullColumnName.replace(".", "_"));
            listIndex++;
            // Generate a ColumnInfo list, to be used by ResultSetWriter for generating the field metadata
            String colArraySize = newField.getArraysize();
            if (colArraySize == null && "CHAR".equals(newField.getDatatype().toUpperCase())) {
            	colArraySize = "1";
            }
            
            ColumnInfo colInfo = new ColumnInfo(resultCol.getName(), getDatatypeClass(resultCol.getDatatype(), colArraySize), newField.description);
    	    colInfo.setUCD(resultCol.ucd);
    	    colInfo.setUtype(resultCol.utype);	
    	    colInfo.setUnitString(resultCol.unit);
    	    colInfo.setXtype(newField.xtype);
            colInfo.setShape(getShape(colArraySize));

    	     
    	    colInfo.setAuxDatum(new DescribedValue(VOStarTable.ID_INFO, newField.id));
    	    colInfo.setAuxDatum(new DescribedValue(new DefaultValueInfo(TABLE_NAME_INFO, String.class),
                    resultCol.tableName));
    	    colInfo.setAuxDatum(new DescribedValue(new DefaultValueInfo(ACTUAL_COLUMN_NAME_INFO, String.class),
                    resultCol.getColumnName()));
    	    
    	    columnInfoList.add(colInfo);
        }
        
        ColumnInfo[] columnInfoArray = columnInfoList.toArray(new ColumnInfo[0]);

        List<String> serviceIDs = determineDatalinks(columnNames);

        resultsResource.setTable(resultsTable);
        votableDocument.getResources().add(resultsResource);

        // Add the "meta" resources to describe services for each columnID in
        // list columnIDs that we recognize
        
        List<VOTableResource> metaResources = generateMetaResources(serviceIDs, columnNames);
        votableDocument.getResources().addAll(metaResources);

        VOTableInfo info = new VOTableInfo("QUERY_STATUS", "OK");
        resultsResource.getInfos().add(info);

        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        Date now = new Date();
        VOTableInfo info2 = new VOTableInfo("QUERY_TIMESTAMP", df.format(now));
        resultsResource.getInfos().add(info2);

        // for documentation, add the query to the table as an info element
        if (queryInfo != null)
        {
            info = new VOTableInfo("QUERY", queryInfo);
            resultsResource.getInfos().add(info);
        }
 
        if (resultSetWriter != null) {
            ((ResultSetWriter) resultSetWriter).setInfos(resultsResource.getInfos());
            ((ResultSetWriter) resultSetWriter).setResources(metaResources);
            ((ResultSetWriter) resultSetWriter).setColumns(columnInfoArray);
            if (maxrec != null) {
                resultSetWriter.write(rs, out, maxrec);
            } else {
                resultSetWriter.write(rs, out);
            }
            this.rowcount = ((ResultSetWriter) resultSetWriter).getRowCount();

        } else if (voDocumentWriter != null) {
            ResultSetTableData tableData = new ResultSetTableData(rs, formats);
            resultsTable.setTableData(tableData);
            if (maxrec != null) {
                voDocumentWriter.write(votableDocument, out, maxrec);
            } else {
                voDocumentWriter.write(votableDocument, out);
            }
            this.rowcount = tableData.getRowCount();
        }
        
        log.debug("Final row count after processing: " + this.rowcount); 

    }
    
    /**
     * Determines the appropriate Java class type for a given TAP data type and array size specification.
     *
     * @param datatype  The TAP data type specification object containing the type information.
     * @param arraysize The size specification for array types. 
     *
     * @return The corresponding Java Class object that represents the specified data type.
     *
     * @throws NullPointerException if datatype is null
     */
	protected static final Class<?> getDatatypeClass(final TapDataType datatype, final String arraysize) {
		boolean isScalar = arraysize == null || (arraysize.length() == 1 && arraysize.equals("1"));
		switch(datatype.getDatatype().toUpperCase()) {
			case "BLOB":
				return boolean[].class;
			case "BOOLEAN":
				return isScalar ? Boolean.class : boolean[].class;
			case "DOUBLE":
				return isScalar ? Double.class : double[].class;
			case "DOUBLECOMPLEX":
				return double[].class;
			case "FLOAT":
				return isScalar ? Float.class : float[].class;
			case "FLOATCOMPLEX":
				return float[].class;
			case "INT":
				return isScalar ? Integer.class : int[].class;
			case "LONG":
				return isScalar ? Long.class : long[].class;
			case "SHORT":
				return isScalar ? Short.class : short[].class;
			case "UNSIGNEDBYTE":
				return isScalar ? Short.class : short[].class;
			case "CHAR":
			case "UNICODECHAR":
			default: /* If the type is not know (theoretically, never happens), return char[*] by default. */
				return isScalar ? Character.class : String.class;
		}
	}

	/**
	 * Convert the given VOTable arraysize into a {@link ColumnInfo} shape.
	 *
	 * @param arraysize	Value of the VOTable attribute "arraysize".
	 *
	 * @return	The corresponding {@link ColumnInfo} shape.
	 */
	protected static final int[] getShape(final String arraysize) {
		if (arraysize == null)
			return new int[0];
		else if (arraysize.charAt(arraysize.length() - 1) == '*')
			return new int[]{ -1 };
		else {
			try {
				return new int[]{ Integer.parseInt(arraysize) };
			} catch(NumberFormatException nfe) {
				return new int[0];
			}
		}
	}

	/**
	 * Generates a list of VOTable meta resources.
	 * 
	 * @param serviceIDs A list of service identifiers used to locate corresponding XML templates.
	 * 
	 * @param columns    A list of column names that will be mapped to template variables.
	 *
	 * @return A list of VOTableResource objects containing the processed meta information
	 *
	 * @throws IOException 
	 * @throws IllegalStateException 
	 */
    private List<VOTableResource> generateMetaResources(List<String> serviceIDs, List<String> columns) throws IOException {
        List<VOTableResource> metaResources = new ArrayList<>();
        for (String serviceID : serviceIDs) {
            Path snippetPath = Path.of(datalinkConfig + serviceID + ".xml");
            String content = Files.readString(snippetPath, StandardCharsets.US_ASCII);
            ST datalinkTemplate = new ST(content, '$', '$');
            datalinkTemplate.add("baseUrl", baseUrl);

            int columnIndex = 0;
            for (String col : columns) {
                datalinkTemplate.add(col, "col_" + columnIndex);
                columnIndex++;
            }

            VOTableReader reader = new VOTableReader();
            VOTableDocument serviceDocument = reader.read(datalinkTemplate.render());
            VOTableResource metaResource = serviceDocument.getResourceByType("meta");
            metaResources.add(metaResource);
        }
        return metaResources;
    }

    private List<String> determineDatalinks(List<String> columns)
        throws IOException
    {
        String content;

        try
        {
            Path manifestPath = Path.of(datalinkConfig + "datalink-manifest.json");
            content = Files.readString(manifestPath, StandardCharsets.US_ASCII);
        }
        catch (IOException e)
        {
            log.warn("failed to open datalink manifest");
            return new ArrayList<String>();
        }

        List<String> datalinks = new ArrayList<String>();
        JsonObject manifest = JsonParser.parseString(content).getAsJsonObject();
        for (Map.Entry<String,JsonElement> entry : manifest.entrySet())
        {
            JsonArray requiredColumnsArray = entry.getValue().getAsJsonArray();
            List<String> requiredColumns = new ArrayList<String>();
            for (JsonElement col: requiredColumnsArray)
            {
                requiredColumns.add(col.getAsString());
            }

            if (columns.containsAll(requiredColumns))
            {
                datalinks.add(entry.getKey());
            }
        }

        return datalinks;
    }

    protected VOTableField createVOTableField(TapSelectItem resultCol)
    {
        if (resultCol != null)
        {
            TapDataType tt = resultCol.getDatatype();
            VOTableField newField = new VOTableField(resultCol.getName(),tt.getDatatype(), tt.arraysize);
            newField.xtype = tt.xtype;
            newField.description = resultCol.description;
            newField.id = resultCol.columnID;
            newField.utype = resultCol.utype;
            newField.ucd = resultCol.ucd;
            newField.unit = resultCol.unit;

            return newField;
        }

        return null;
    }
    
    @Override
    public VOTableDocument generateOutputTable() throws IOException {
        return null;
    }
}

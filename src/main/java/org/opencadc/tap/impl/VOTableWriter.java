/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2011.                            (c) 2011.
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

package org.opencadc.tap.impl;

import ca.nrc.cadc.dali.tables.TableWriter;
import ca.nrc.cadc.dali.tables.votable.FieldElement;
import ca.nrc.cadc.dali.tables.votable.GroupElement;
import ca.nrc.cadc.dali.tables.votable.ParamElement;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableGroup;
import ca.nrc.cadc.dali.tables.votable.VOTableInfo;
import ca.nrc.cadc.dali.tables.votable.VOTableParam;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.dali.util.Format;
import ca.nrc.cadc.dali.util.FormatFactory;
import ca.nrc.cadc.xml.ContentConverter;
import ca.nrc.cadc.xml.IterableContent;
import ca.nrc.cadc.xml.MaxIterations;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.output.XMLOutputter;


/**
 * Basic VOTable reader. This class currently supports a subset of VOTable (tabledata
 * only) and always writes with the VOTable-1.2 namespace. TODO: complete support and
 * allow caller to specify the target namespace.
 *
 * @author pdowler
 */
public class VOTableWriter implements TableWriter<VOTableDocument> {

    private static final Logger log = Logger.getLogger(VOTableWriter.class);

    public static final String CONTENT_TYPE = "application/x-votable+xml";
    public static final String CONTENT_TYPE_ALT = "text/xml";

    // Output format options
    public enum XMLLayout {
        COMPACT,
        PRETTY
    }

    // VOTable Version number.
    public static final String VOTABLE_VERSION = "1.4";

    // Uri to the XML schema.
    public static final String XSI_SCHEMA = "http://www.w3.org/2001/XMLSchema-instance";

    // Uri to the VOTable schema.
    public static final String VOTABLE_11_NS_URI = "http://www.ivoa.net/xml/VOTable/v1.1";
    public static final String VOTABLE_12_NS_URI = "http://www.ivoa.net/xml/VOTable/v1.2";
    public static final String VOTABLE_13_NS_URI = "http://www.ivoa.net/xml/VOTable/v1.3";

    private FormatFactory formatFactory;

    private boolean binaryTable;
    private String mimeType;
    private XMLLayout xmlLayout;

    /**
     * Default constructor.
     */
    public VOTableWriter() {
        this(false, null, XMLLayout.PRETTY);
    }
    
    /**
     * Constructor with specified mime type and pretty printing.
     */
    public VOTableWriter(String mimeType) {
        this(false, mimeType, XMLLayout.PRETTY);
    }

    /**
     * Constructor with specified mime type and xml layout.
     */
    public VOTableWriter(String mimeType, XMLLayout xmlLayout) {
        this(false, mimeType, XMLLayout.PRETTY);
    }

    /**
     * Create a VOTableWriter and optionally support binary TABLEDATA cells (not
     * yet implemented.)
     *
     * @param binaryTable
     */
    public VOTableWriter(boolean binaryTable, String mimeType) {
        this.binaryTable = binaryTable;
        this.mimeType = mimeType;
        this.xmlLayout = XMLLayout.PRETTY;
    }
    
    /**
     * Full constructor with all options.
     *
     * @param binaryTable
     * @param mimeType
     * @param format
     */
    public VOTableWriter(boolean binaryTable, String mimeType, XMLLayout format) {
        this.binaryTable = binaryTable;
        this.mimeType = mimeType;
        this.xmlLayout = format;
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
     * Set the output format.
     *
     * @param format The desired output format
     */
    public void setOutputFormat(XMLLayout format) {
        this.xmlLayout = format;
    }
    
    /**
     * Write the VOTable to the specified OutputStream.
     *
     * @param votable VOTable object to write.
     * @param ostream OutputStream to write to.
     * @throws IOException if problem writing to OutputStream.
     */
    @Override
    public void write(VOTableDocument votable, OutputStream ostream)
            throws IOException {
        write(votable, ostream, Long.MAX_VALUE);
    }

    /**
     * Write the VOTable to the specified OutputStream, only writing maxrec rows.
     * If the VOTable contains more than maxrec rows, appends an INFO element with
     * name="QUERY_STATUS" value="OVERFLOW" to the VOTable.
     *
     * @param votable VOTable object to write.
     * @param ostream OutputStream to write to.
     * @param maxrec maximum number of rows to write.
     * @throws IOException if problem writing to OutputStream.
     */
    @Override
    public void write(VOTableDocument votable, OutputStream ostream, Long maxrec)
            throws IOException {
        Writer writer = new BufferedWriter(new OutputStreamWriter(ostream, "UTF-8"));
        write(votable, writer, maxrec);
    }
    
    /**
     * Write the VOTable to the specified Writer.
     *
     * @param votable VOTable object to write.
     * @param writer Writer to write to.
     * @throws IOException if problem writing to the writer.
     */
    @Override
    public void write(VOTableDocument votable, Writer writer)
            throws IOException {
        write(votable, writer, Long.MAX_VALUE);
    }

    /**
     * Write the VOTable to the specified Writer, only writing maxrec rows.
     * If the VOTable contains more than maxrec rows, appends an INFO element with
     * name="QUERY_STATUS" value="OVERFLOW" to the VOTable.
     *
     * @param votable VOTable object to write.
     * @param writer Writer to write to.
     * @param maxrec maximum number of rows to write.
     * @throws IOException if problem writing to the writer.
     */
    @Override
    public void write(VOTableDocument votable, Writer writer, Long maxrec)
            throws IOException {
        if (formatFactory == null) {
            this.formatFactory = new FormatFactory();
        }
        writeImpl(votable, writer, maxrec);
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
        if (xmlLayout == XMLLayout.COMPACT) {
            outputter.setFormat(org.jdom2.output.Format.getCompactFormat());
        } else {
            outputter.setFormat(org.jdom2.output.Format.getPrettyFormat());
        }
        outputter.output(document, output);
    }

    protected void writeImpl(VOTableDocument votable, Writer writer, Long maxrec)
            throws IOException {
        log.debug("write, maxrec=" + maxrec);

        // VOTable document and root element.
        Document document = createDocument();
        Element root = document.getRootElement();
        Namespace namespace = root.getNamespace();

        
        for (VOTableResource votResource : votable.getResources()) {
            Element resource = createResource(votResource, namespace);
            root.addContent(resource);

            if (votResource.getTable() != null) {
                VOTableTable vot = votResource.getTable();

                // Create the TABLE element and add to the RESOURCE element.
                Element table = new Element("TABLE", namespace);
                resource.addContent(table);

                // Create the INFO element and add to the RESOURCE element.
                for (VOTableInfo in : vot.getInfos()) {
                    Element info = new Element("INFO", namespace);
                    info.setAttribute("name", in.getName());
                    info.setAttribute("value", in.getValue());
                    log.debug("INFO content: " + in.content);
                    if (in.content != null) {
                        info.setText(in.content);
                    }
                    table.addContent(info);
                }
                log.debug("wrote resource.table.info: " + vot.getInfos().size());

                // Add the metadata elements.
                for (VOTableParam param : vot.getParams()) {
                    table.addContent(new ParamElement(param, namespace));
                }
                log.debug("wrote resource.table.param: " + vot.getParams().size());
                for (VOTableField field : vot.getFields()) {
                    table.addContent(new FieldElement(field, namespace));
                }
                log.debug("wrote resource.table.field: " + vot.getFields().size());

                if (vot.getTableData() != null) {
                    // Create the DATA and TABLEDATA elements.
                    Element data = new Element("DATA", namespace);
                    table.addContent(data);

                    log.debug("setup content interator: maxrec=" + maxrec);
                    Element trailer = new Element("INFO", namespace);
                    trailer.setAttribute("name", "placeholder");
                    trailer.setAttribute("value", "ignore");
                    resource.addContent(trailer);

                    Iterator<List<Object>> rowIter = vot.getTableData().iterator();

                    TabledataContentConverter elementConverter = new TabledataContentConverter(vot.getFields(), namespace, trailer);
                    TabledataMaxIterations maxIterations = new TabledataMaxIterations(maxrec, trailer);

                    IterableContent<Element, List<Object>> tabledata
                            = new IterableContent<Element, List<Object>>("TABLEDATA", namespace, rowIter, elementConverter, maxIterations);

                    data.addContent(tabledata);
                    
                }
            }
        }

        // Write out the VOTABLE.
        XMLOutputter outputter = new XMLOutputter();
        if (xmlLayout == XMLLayout.COMPACT) {
            outputter.setFormat(org.jdom2.output.Format.getCompactFormat());
        } else {
            outputter.setFormat(org.jdom2.output.Format.getPrettyFormat());
        }
        outputter.output(document, writer);
    }

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

    // Build a String containing the nested Exception messages.
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
        return sb.toString();
    }

    private class TabledataMaxIterations implements MaxIterations {

        private final long maxRec;
        private final Element info;

        TabledataMaxIterations(Long maxRec, Element info) {
            if (maxRec == null) {
                this.maxRec = Long.MAX_VALUE;
            } else {
                this.maxRec = maxRec;
            }
            this.info = info;
        }

        @Override
        public long getMaxIterations() {
            return maxRec;
        }

        @Override
        public void maxIterationsReached(boolean moreAvailable) {
            log.warn("TabledataMaxIterations.maxIterationsReached: " + maxRec + ", more=" + moreAvailable);
            if (moreAvailable) {
                // DALI overflow
                info.setAttribute("name", "QUERY_STATUS");
                info.setAttribute("value", "OVERFLOW");
            }
        }

    }

    private class TabledataContentConverter implements ContentConverter<Element, List<Object>> {

        private final List<VOTableField> fields;
        private final Namespace namespace;
        private final List<Format<Object>> formats;
        private final Element trailer;

        TabledataContentConverter(List<VOTableField> fields, Namespace namespace, Element trailer) {
            this.fields = fields;
            this.namespace = namespace;
            this.trailer = trailer;
            
            // initialize the list of associated formats
            this.formats = new ArrayList<Format<Object>>(fields.size());

            for (VOTableField field : fields) {
                Format<Object> format = null;
                if (field.getFormat() == null) {
                    format = formatFactory.getFormat(field);
                } else {
                    format = field.getFormat();
                }
                formats.add(format);
            }
        }

        @Override
        public Element convert(List<Object> row) {
            if (row.size() != fields.size()) {
                throw new IllegalStateException("cannot write row: " + fields.size()
                        + " metadata fields, " + row.size() + " data columns");
            }

            // TR element.
            Element tr = new Element("TR", namespace);

            // TD elements.
            for (int i = 0; i < row.size(); i++) {
                try {
                    Object o = row.get(i);
                    Format<Object> fmt = formats.get(i);
                    Element td = new Element("TD", namespace);
                    td.setText(fmt.format(o));
                    tr.addContent(td);
                } catch (Exception ex) {
                    log.debug("failure while iterating", ex);
                    // DALI error
                    trailer.setAttribute("name", "QUERY_STATUS");
                    trailer.setAttribute("value", "ERROR");
                    trailer.setText(ex.toString());
                    throw ex;
                }
            }

            return tr;

        }

    }

}


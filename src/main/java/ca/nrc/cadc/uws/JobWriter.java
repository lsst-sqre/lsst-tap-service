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

package ca.nrc.cadc.uws;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.xml.XmlUtil;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

/**
 * Writes a Job as XML to an output.
 *
 * @author Sailor Zhang
 */
public class JobWriter {

    private static Logger log = Logger.getLogger(JobWriter.class);

    private DateFormat dateFormat;

    public JobWriter() {
        this.dateFormat = UWS.getDateFormat();
    }

    /**
     * Write to root Element to a writer.
     *
     * @param root Root Element to write.
     * @param writer Writer to write to.
     * @throws IOException if the writer fails to write.
     */
    protected void writeDocument(Element root, Writer writer)
            throws IOException {
        XMLOutputter outputter = new XMLOutputter();
        outputter.setFormat(Format.getPrettyFormat());
        Document document = new Document(root);
        outputter.output(document, writer);
    }

    /**
     * Write the job to an OutputStream.
     *
     * @param job
     * @param out OutputStream to write to.
     * @throws IOException if the writer fails to write.
     */
    public void write(Job job, OutputStream out)
            throws IOException {
        write(job, new OutputStreamWriter(out));
    }

    /**
     * Write the job to a writer.
     *
     * @param job
     * @param writer Writer to write to.
     * @throws IOException if the writer fails to write.
     */
    public void write(Job job, Writer writer)
            throws IOException {
        Element root = getRootElement(job);
        XMLOutputter outputter = new XMLOutputter();
        outputter.setFormat(Format.getPrettyFormat());
        Document document = new Document(root);
        outputter.output(document, writer);
    }

    public void writeParametersDoc(List<Parameter> params, OutputStream out)
            throws IOException {
        writeParametersDoc(params, new OutputStreamWriter(out));
    }

    public void writeParametersDoc(List<Parameter> params, Writer writer) throws IOException {
        Element root = getParametersRootElement(params);
        XMLOutputter outputter = new XMLOutputter();
        outputter.setFormat(Format.getPrettyFormat());
        Document document = new Document(root);
        outputter.output(document, writer);
    }

    public void writeResultsDoc(List<Result> params, OutputStream out)
            throws IOException {
        writeResultsDoc(params, new OutputStreamWriter(out));
    }

    public void writeResultsDoc(List<Result> results, Writer writer) throws IOException {
        Element root = getResultsRootElement(results);
        XMLOutputter outputter = new XMLOutputter();
        outputter.setFormat(Format.getPrettyFormat());
        Document document = new Document(root);
        outputter.output(document, writer);
    }

    /**
     * Get an Element representing a job Element.
     *
     * @return A job Element.
     */
    public static Element getJob() {
        Element element = new Element(JobAttribute.JOB.getValue(), UWS.NS);
        element.addNamespaceDeclaration(UWS.NS);
        element.addNamespaceDeclaration(UWS.XLINK_NS);
        //element.setAttribute("schemaLocation", "http://www.ivoa.net/xml/UWS/v1.0 UWS.xsd", UWS.XSI_NS);
        return element;
    }

    public Element getParametersRootElement(List<Parameter> params) {
        Element root = getParameters(params);
        root.addNamespaceDeclaration(UWS.NS);
        root.addNamespaceDeclaration(UWS.XLINK_NS);
        root.setAttribute(JobAttribute.VERSION.getValue(), UWS.UWS_VERSION);
        return root;
    }

    public Element getResultsRootElement(List<Result> results) {
        Element root = getResults(results);
        root.addNamespaceDeclaration(UWS.NS);
        root.addNamespaceDeclaration(UWS.XLINK_NS);
        root.setAttribute(JobAttribute.VERSION.getValue(), UWS.UWS_VERSION);
        return root;
    }

    /**
     * Create the root element of a job document.
     *
     * @param job
     * @return
     */
    public Element getRootElement(Job job) {
        Element root = new Element(JobAttribute.JOB.getValue(), UWS.NS);
        root.addNamespaceDeclaration(UWS.NS);
        root.addNamespaceDeclaration(UWS.XLINK_NS);
        root.setAttribute(JobAttribute.VERSION.getValue(), UWS.UWS_VERSION);

        root.addContent(getJobId(job));
        root.addContent(getRunId(job));
        root.addContent(getOwnerId(job));
        root.addContent(getPhase(job));
        root.addContent(getQuote(job));
        Element creationTime = getCreationTime(job);
        if (creationTime != null) {
            root.addContent(creationTime);
        }
        root.addContent(getStartTime(job));
        root.addContent(getEndTime(job));
        root.addContent(getExecutionDuration(job));
        root.addContent(getDestruction(job));
        root.addContent(getParameters(job.getParameterList()));
        root.addContent(getResults(job.getResultsList()));
        Element errorSummary = getErrorSummary(job);
        if (errorSummary != null) {
            root.addContent(errorSummary);
        }

        Element jobInfo = getJobInfo(job);
        if (jobInfo != null) {
            root.addContent(jobInfo);
        }

        return root;
    }

    /**
     * Get an Element representing the Job jobId.
     *
     * @return The Job jobId Element.
     */
    public Element getJobId(Job job) {
        Element element = new Element(JobAttribute.JOB_ID.getValue(), UWS.NS);
        element.addContent(job.getID());
        return element;
    }

    /**
     * Get an Element representing the Job jobref.
     *
     * @param host The host part of the Job request URL.
     * @return The Job jobref Element.
     */
    public Element getJobRef(String host, Job job) {
        Element element = new Element(JobAttribute.JOB_REF.getValue(), UWS.NS);
        element.setAttribute("id", job.getID());
        element.setAttribute("xlink:href", host + job.getRequestPath() + "/" + job.getID());
        return element;
    }

    /**
     * Get an Element representing the Job runId.
     *
     * @return The Job runId Element.
     */
    public Element getRunId(Job job) {
        Element element = new Element(JobAttribute.RUN_ID.getValue(), UWS.NS);
        element.addContent(job.getRunID());
        return element;
    }

    /**
     * Get an Element representing the Job ownerId.
     *
     * @return The Job ownerId Element.
     */
    public Element getOwnerId(Job job) {
        Element element = new Element(JobAttribute.OWNER_ID.getValue(), UWS.NS);
        if (job.ownerDisplay != null) {
            element.addContent(job.ownerDisplay);
        } else {
            element.setAttribute("nil", "true", UWS.XSI_NS);
        }
        return element;
    }

    /**
     * Get an Element representing the Job phase.
     *
     * @return The Job phase Element.
     */
    public Element getPhase(Job job) {
        Element element = new Element(JobAttribute.EXECUTION_PHASE.getValue(), UWS.NS);
        element.addContent(job.getExecutionPhase().toString());
        return element;
    }

    /**
     * Get an Element representing the Job quote.
     *
     * @return The Job quote Element.
     */
    public Element getQuote(Job job) {
        Element element = new Element(JobAttribute.QUOTE.getValue(), UWS.NS);
        Date date = job.getQuote();
        if (date == null) {
            element.setAttribute("nil", "true", UWS.XSI_NS);
        } else {
            element.addContent(dateFormat.format(date));
        }
        return element;
    }

    /**
     * Get an Element representing the Job startTime.
     *
     * @return The Job startTime Element.
     */
    public Element getStartTime(Job job) {
        Element element = new Element(JobAttribute.START_TIME.getValue(), UWS.NS);
        Date date = job.getStartTime();
        if (date == null) {
            element.setAttribute("nil", "true", UWS.XSI_NS);
        } else {
            element.addContent(dateFormat.format(date));
        }
        return element;
    }

    /**
     * Get an Element representing the Job endTime.
     *
     * @return The Job endTime Element.
     */
    public Element getEndTime(Job job) {
        Element element = new Element(JobAttribute.END_TIME.getValue(), UWS.NS);
        Date date = job.getEndTime();
        if (date == null) {
            element.setAttribute("nil", "true", UWS.XSI_NS);
        } else {
            element.addContent(dateFormat.format(date));
        }
        return element;
    }

    /**
     * Get an Element representing the Job creationTime.
     *
     * @return The Job creationTime Element.
     */
    public Element getCreationTime(Job job) {

        Date date = job.getCreationTime();
        if (date != null) {
            Element element = new Element(JobAttribute.CREATION_TIME.getValue(), UWS.NS);
            element.addContent(dateFormat.format(date));
            return element;
        }
        return null;
    }

    /**
     * Get an Element representing the Job executionDuration.
     *
     * @return The Job executionDuration Element.
     */
    public Element getExecutionDuration(Job job) {
        Element element = new Element(JobAttribute.EXECUTION_DURATION.getValue(), UWS.NS);
        element.addContent(Long.toString(job.getExecutionDuration()));
        return element;
    }

    /**
     * Get an Element representing the Job destruction.
     *
     * @return The Job destruction Element.
     */
    public Element getDestruction(Job job) {
        Element element = new Element(JobAttribute.DESTRUCTION_TIME.getValue(), UWS.NS);
        Date date = job.getDestructionTime();
        if (date == null) {
            element.setAttribute("nil", "true", UWS.XSI_NS);
        } else {
            element.addContent(dateFormat.format(date));
        }
        return element;
    }

    /**
     * Get an Element representing the Job parameters.
     *
     * @return The Job parameters Element.
     */
    public Element getParameters(List<Parameter> params) {
        Element element = new Element(JobAttribute.PARAMETERS.getValue(), UWS.NS);
        for (Parameter parameter : params) {
            Element e = new Element(JobAttribute.PARAMETER.getValue(), UWS.NS);
            e.setAttribute("id", parameter.getName());
            e.addContent(parameter.getValue());
            element.addContent(e);
        }
        return element;
    }

    /**
     * Get an Element representing the Job results.
     *
     * @return The Job results Element.
     */
    public Element getResults(List<Result> results) {
        Element element = new Element(JobAttribute.RESULTS.getValue(), UWS.NS);
        for (Result result : results) {
            Element e = new Element(JobAttribute.RESULT.getValue(), UWS.NS);
            e.setAttribute("id", result.getName());
            e.setAttribute("href", result.getURI().toASCIIString(), UWS.XLINK_NS);
            element.addContent(e);
        }
        return element;
    }

    /**
     * Get an Element representing the Job errorSummary.
     *
     * @return The Job errorSummary Element.
     */
    public Element getErrorSummary(Job job) {
        Element eleErrorSummary = null;
        ErrorSummary es = job.getErrorSummary();
        if (es != null) {
            eleErrorSummary = new Element(JobAttribute.ERROR_SUMMARY.getValue(), UWS.NS);
            eleErrorSummary.setAttribute("type", es.getErrorType().toString().toLowerCase());
            eleErrorSummary.setAttribute("hasDetail", Boolean.toString(es.getHasDetail()));

            Element eleMessage = new Element(JobAttribute.ERROR_SUMMARY_MESSAGE.getValue(), UWS.NS);
            eleMessage.addContent(job.getErrorSummary().getSummaryMessage());
            eleErrorSummary.addContent(eleMessage);
        }
        return eleErrorSummary;
    }

    /**
     * Get an Element representing the Job jobInfo.
     *
     * @return The Job jobInfo Element.
     */
    public Element getJobInfo(Job job) {
        Element element = null;
        JobInfo jobInfo = job.getJobInfo();
        
        if (jobInfo != null && jobInfo.getContent() != null) {
            element = new Element(JobAttribute.JOB_INFO.getValue(), UWS.NS);
            String contentType = jobInfo.getContentType();
            
            // If contentType is text/plain, just add the content as text without parsing
            if (contentType != null && contentType.startsWith("text/plain")) {
                log.debug("getJobInfo: treating content as plain text (contentType=" + contentType + ")");
                element.addContent(jobInfo.getContent());
            } else {
                try {
                    log.debug("getJobInfo: attempting to parse content as XML (contentType=" + contentType + ")");
                    Document doc = XmlUtil.buildDocument(jobInfo.getContent());
                    element.addContent(doc.getRootElement().detach());
                    log.debug("getJobInfo: successfully parsed as complete XML document");
                } catch (Exception e) {
                    log.debug("Failed to parse JobInfo content as complete XML document: " + e.getMessage());
                    // If it's not a complete document, try to parse as XML fragment
                    // by wrapping it in a temporary root element
                    try {
                        String wrapped = "<wrapper>" + jobInfo.getContent() + "</wrapper>";
                        Document doc = XmlUtil.buildDocument(wrapped);
                        // Extract the children of the wrapper and add them directly to jobInfo
                        for (org.jdom2.Content content : doc.getRootElement().getContent()) {
                            element.addContent((org.jdom2.Content) content.clone());
                        }
                        log.debug("Successfully parsed JobInfo content as XML fragment and added children directly");
                    } catch (Exception e2) {
                        log.debug("Failed to parse JobInfo content as XML fragment: " + e2.getMessage());
                        // If all XML parsing fails, just include it as plain text
                        element.addContent(jobInfo.getContent());
                        log.debug("Including JobInfo content as plain text");
                    }
                }
            }
        }
        return element;
    }
}
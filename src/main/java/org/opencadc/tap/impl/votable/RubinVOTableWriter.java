package org.opencadc.tap.impl.votable;

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
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.dali.tables.votable.XMLOutputProcessor;
import ca.nrc.cadc.dali.util.FormatFactory;
import ca.nrc.cadc.xml.MaxIterations;

import org.apache.log4j.Logger;
import org.jdom2.Comment;
import org.jdom2.Content;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.output.XMLOutputter;

import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Extension of {@link VOTableWriter} that injects {@code <COOSYS>} and
 * {@code <TIMESYS>} elements into the {@code results} RESOURCE, and honours
 * the {@code ref} attribute already set on each FIELD by the caller.
 *
 * <p>When no coordinate / time metadata is supplied the parent implementation
 * is delegated to directly; this class adds zero overhead for queries that
 * return no positional or temporal columns.
 *
 * <p><b>Upstream coupling note</b>: {@link #writeImpl} duplicates logic from
 * {@code VOTableWriter.writeImpl()} (cadc-dali 1.2.28) because
 * {@code createResource()} and {@code createInfo()} are {@code private} there.
 * Review this method when upgrading cadc-dali.
 */
public class RubinVOTableWriter extends VOTableWriter {

    private static final Logger log = Logger.getLogger(RubinVOTableWriter.class);

    private final SerializationType mySerialization;
    private FormatFactory myFormatFactory;
    private CoordSysMetadata coordSysMetadata;

    public RubinVOTableWriter() {
        this(SerializationType.TABLEDATA);
    }

    public RubinVOTableWriter(SerializationType serialization) {
        super(serialization);
        this.mySerialization = serialization;
    }

    /** Set the coordinate/time system metadata to inject. May be null (no-op). */
    public void setCoordSysMetadata(CoordSysMetadata metadata) {
        this.coordSysMetadata = metadata;
    }

    /**
     * Capture a local reference to the format factory so it is available inside
     * the overridden {@link #writeImpl}.
     */
    @Override
    public void setFormatFactory(FormatFactory ff) {
        super.setFormatFactory(ff);
        this.myFormatFactory = ff;
    }

    // -----------------------------------------------------------------------
    // Core override
    // -----------------------------------------------------------------------

    /**
     * Override that injects COOSYS / TIMESYS elements into the {@code results}
     * RESOURCE.  When there is nothing to inject the parent implementation is
     * called instead to avoid any code-duplication overhead.
     *
     * <p>NOTE: this method duplicates the logic of
     * {@code VOTableWriter.writeImpl()} (cadc-dali 1.2.28) because
     * {@code createResource()} and {@code createInfo()} are {@code private}
     * in the parent.  The private {@code TabledataMaxIterations} is replaced
     * by the local {@link OverflowTracker} inner class.
     */
    @Override
    protected void writeImpl(VOTableDocument votable, Writer writer, Long maxrec)
            throws IOException {

        if (coordSysMetadata == null || coordSysMetadata.isEmpty()) {
            super.writeImpl(votable, writer, maxrec);
            return;
        }

        if (myFormatFactory == null) {
            myFormatFactory = new FormatFactory();
        }

        Document document = createDocument();
        Element root = document.getRootElement();
        Namespace namespace = root.getNamespace();

        Iterator<List<Object>> rowIter = null;
        List<VOTableField> fields = null;
        Element trailer = null;
        OverflowTracker maxIterations = null;

        for (VOTableResource votResource : votable.getResources()) {
            Element resource = buildResource(votResource, namespace);

            // Inject COOSYS / TIMESYS elements only into the results resource.
            if ("results".equals(votResource.getType())) {
                injectCoordSysElements(resource, namespace);
            }

            root.addContent(resource);

            if (votResource.getTable() != null) {
                VOTableTable vot = votResource.getTable();

                Element table = new Element("TABLE", namespace);
                resource.addContent(table);

                for (VOTableInfo tableInfo : vot.getInfos()) {
                    table.addContent(buildInfo(tableInfo, namespace));
                }
                for (VOTableParam param : vot.getParams()) {
                    table.addContent(new ParamElement(param, namespace));
                }
                for (VOTableField field : vot.getFields()) {
                    table.addContent(new FieldElement(field, namespace));
                }

                if (vot.getTableData() != null) {
                    Element data = new Element("DATA", namespace);
                    table.addContent(data);

                    trailer = new Element("INFO", namespace);
                    trailer.setAttribute("name", "placeholder");
                    trailer.setAttribute("value", "ignore");
                    resource.addContent(trailer);

                    maxIterations = new OverflowTracker(maxrec, trailer);
                    rowIter = vot.getTableData().iterator();
                    fields = vot.getFields();

                    if (SerializationType.TABLEDATA.equals(mySerialization)) {
                        data.addContent(new Element("TABLEDATA", namespace));
                    } else if (SerializationType.BINARY2.equals(mySerialization)) {
                        data.addContent(new Element("BINARY2", namespace));
                    } else {
                        throw new RuntimeException("Unsupported serialization: " + mySerialization);
                    }
                } else {
                    table.addContent(new Comment("data goes here"));
                }
            }
        }

        for (VOTableInfo votableInfo : votable.getInfos()) {
            root.addContent(buildInfo(votableInfo, namespace));
        }

        try {
            XMLOutputter outputter = new XMLOutputter();
            outputter.setFormat(org.jdom2.output.Format.getPrettyFormat());
            outputter.setXMLOutputProcessor(
                    new XMLOutputProcessor(rowIter, fields, maxIterations, trailer, myFormatFactory));
            outputter.output(document, writer);
        } catch (RuntimeException ex) {
            log.error("OUTPUT FAIL", ex);
            throw ex;
        }
    }

    // -----------------------------------------------------------------------
    // Replication of private VOTableWriter helpers
    // -----------------------------------------------------------------------

    /** Mirrors the private {@code createResource()} method in VOTableWriter. */
    private Element buildResource(VOTableResource votResource, Namespace namespace) {
        Element resource = new Element("RESOURCE", namespace);
        resource.setAttribute("type", votResource.getType());

        if (votResource.id != null) {
            resource.setAttribute("ID", votResource.id);
        }
        if (votResource.getName() != null) {
            resource.setAttribute("name", votResource.getName());
        }
        if (votResource.utype != null) {
            resource.setAttribute("utype", votResource.utype);
        }
        if (votResource.description != null) {
            Element description = new Element("DESCRIPTION", namespace);
            description.setText(votResource.description);
            resource.addContent(description);
        }
        for (VOTableInfo in : votResource.getInfos()) {
            resource.addContent(buildInfo(in, namespace));
        }
        for (VOTableParam param : votResource.getParams()) {
            resource.addContent(new ParamElement(param, namespace));
        }
        for (VOTableGroup vg : votResource.getGroups()) {
            resource.addContent(new GroupElement(vg, namespace));
        }
        return resource;
    }

    /** Mirrors the private {@code createInfo()} method in VOTableWriter. */
    private Element buildInfo(VOTableInfo voTableInfo, Namespace namespace) {
        Element info = new Element("INFO", namespace);
        info.setAttribute("name", voTableInfo.getName());
        info.setAttribute("value", voTableInfo.getValue());
        if (voTableInfo.id != null) {
            info.setAttribute("ID", voTableInfo.id);
        }
        if (voTableInfo.content != null) {
            info.setText(voTableInfo.content);
        }
        return info;
    }

    // -----------------------------------------------------------------------
    // COOSYS / TIMESYS injection
    // -----------------------------------------------------------------------

    /**
     * Insert COOSYS and TIMESYS child elements into {@code resource} at the
     * correct position: after all DESCRIPTION and INFO siblings but before the
     * first PARAM, GROUP, or TABLE element.
     */
    private void injectCoordSysElements(Element resource, Namespace namespace) {
        List<Content> contents = resource.getContent();

        // Walk existing children to find the last INFO / DESCRIPTION position.
        int insertIndex = 0;
        for (int i = 0; i < contents.size(); i++) {
            Content c = contents.get(i);
            if (c instanceof Element) {
                String name = ((Element) c).getName();
                if ("DESCRIPTION".equals(name) || "INFO".equals(name)) {
                    insertIndex = i + 1;
                } else {
                    // First non-INFO/DESCRIPTION element: stop scanning.
                    break;
                }
            }
        }

        for (Map.Entry<String, CoordSysMetadata.CoosysDef> entry
                : coordSysMetadata.getCoosysDefs().entrySet()) {
            CoordSysMetadata.CoosysDef def = entry.getValue();
            Element coosys = new Element("COOSYS", namespace);
            coosys.setAttribute("ID", def.id);
            coosys.setAttribute("system", def.system);
            if (def.equinox != null) {
                coosys.setAttribute("equinox", def.equinox);
            }
            if (def.epoch != null) {
                coosys.setAttribute("epoch", def.epoch);
            }
            contents.add(insertIndex++, coosys);
        }

        for (Map.Entry<String, CoordSysMetadata.TimesysDef> entry
                : coordSysMetadata.getTimesysDefs().entrySet()) {
            CoordSysMetadata.TimesysDef def = entry.getValue();
            Element timesys = new Element("TIMESYS", namespace);
            timesys.setAttribute("ID", def.id);
            timesys.setAttribute("timescale", def.timescale);
            timesys.setAttribute("refposition", def.refposition);
            contents.add(insertIndex++, timesys);
        }
    }

    // -----------------------------------------------------------------------
    // Inner class mirroring the private TabledataMaxIterations in VOTableWriter
    // -----------------------------------------------------------------------

    private static final class OverflowTracker implements MaxIterations {

        private final long maxRec;
        private final Element info;

        OverflowTracker(Long maxRec, Element info) {
            this.maxRec = (maxRec != null) ? maxRec : Long.MAX_VALUE;
            this.info = info;
        }

        @Override
        public long getMaxIterations() {
            return maxRec;
        }

        @Override
        public void maxIterationsReached(boolean moreAvailable) {
            log.debug("OverflowTracker.maxIterationsReached: maxRec=" + maxRec + ", more=" + moreAvailable);
            if (moreAvailable) {
                info.setAttribute("name", "QUERY_STATUS");
                info.setAttribute("value", "OVERFLOW");
            }
        }

        private static final Logger log = Logger.getLogger(OverflowTracker.class);
    }
}

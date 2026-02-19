package org.opencadc.tap.impl.votable;

import ca.nrc.cadc.dali.tables.votable.VOTableField;

import java.util.List;

/**
 * Stateless utility that inspects a list of VOTableField UCDs and produces a
 * {@link CoordSysMetadata} describing which COOSYS and TIMESYS elements should be
 * emitted in the VOTable output, and which fields should carry a {@code ref} attribute
 * pointing to those elements.
 *
 * <p>UCD matching follows the IVOA UCD1+ vocabulary.  Compound UCDs (e.g.
 * {@code pos.eq.ra;meta.main}) are supported: the primary atom (the part before the
 * first {@code ;}) is extracted for matching.  Matching is case-insensitive.
 *
 * <p>Default coordinate and time system choices for LSST:
 * <ul>
 *   <li>Equatorial positions → ICRS (no equinox attribute required)</li>
 *   <li>Timestamps          → TAI timescale, GEOCENTER reference position</li>
 * </ul>
 */
public class UcdCoordDetector {

    // COOSYS IDs and VOTable system attribute values
    private static final String ICRS_ID          = "icrs";
    private static final String ICRS_SYSTEM      = "ICRS";
    private static final String GALACTIC_ID      = "galactic";
    private static final String GALACTIC_SYSTEM  = "galactic";
    private static final String ECL_FK5_ID       = "ecl_fk5";
    private static final String ECL_FK5_SYSTEM   = "ecl_FK5";
    private static final String SUPERGAL_ID      = "supergalactic";
    private static final String SUPERGAL_SYSTEM  = "supergalactic";

    // TIMESYS ID and attribute values
    private static final String TAI_GEO_ID       = "tai_geo";
    private static final String TAI_TIMESCALE    = "TAI";
    private static final String GEOCENTER_REFPOS = "GEOCENTER";

    private UcdCoordDetector() {
        // static utility class
    }

    /**
     * Analyse the given fields and return the detected coordinate/time system metadata.
     *
     * @param fields ordered list of VOTableField objects (may be empty, never null)
     * @return populated {@link CoordSysMetadata}; never null, may be empty
     */
    public static CoordSysMetadata detect(List<VOTableField> fields) {
        CoordSysMetadata metadata = new CoordSysMetadata();

        for (int i = 0; i < fields.size(); i++) {
            VOTableField field = fields.get(i);
            if (field.ucd == null || field.ucd.isEmpty()) {
                continue;
            }

            // Extract the primary UCD atom from compound UCDs (e.g. "pos.eq.ra;meta.main")
            String primaryUcd = primaryAtom(field.ucd);

            String refId = detectSpatialRef(primaryUcd, metadata);
            if (refId == null) {
                refId = detectTimeRef(primaryUcd, metadata);
            }

            if (refId != null) {
                metadata.setFieldRef(i, refId);
            }
        }

        return metadata;
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private static String primaryAtom(String ucd) {
        int semi = ucd.indexOf(';');
        String atom = (semi >= 0) ? ucd.substring(0, semi) : ucd;
        return atom.toLowerCase().trim();
    }

    private static String detectSpatialRef(String ucd, CoordSysMetadata meta) {
        if (ucd.equals("pos.eq.ra") || ucd.equals("pos.eq.dec")) {
            meta.addCoosys(new CoordSysMetadata.CoosysDef(ICRS_ID, ICRS_SYSTEM));
            return ICRS_ID;
        }
        if (ucd.equals("pos.galactic.lon") || ucd.equals("pos.galactic.lat")) {
            meta.addCoosys(new CoordSysMetadata.CoosysDef(GALACTIC_ID, GALACTIC_SYSTEM));
            return GALACTIC_ID;
        }
        if (ucd.equals("pos.ecliptic.lon") || ucd.equals("pos.ecliptic.lat")) {
            meta.addCoosys(new CoordSysMetadata.CoosysDef(ECL_FK5_ID, ECL_FK5_SYSTEM));
            return ECL_FK5_ID;
        }
        if (ucd.equals("pos.supergalactic.lon") || ucd.equals("pos.supergalactic.lat")) {
            meta.addCoosys(new CoordSysMetadata.CoosysDef(SUPERGAL_ID, SUPERGAL_SYSTEM));
            return SUPERGAL_ID;
        }
        return null;
    }

    private static String detectTimeRef(String ucd, CoordSysMetadata meta) {
        // Durations are not timestamps; exclude them explicitly.
        if (ucd.startsWith("time.duration")) {
            return null;
        }
        if (ucd.equals("time")
                || ucd.equals("time.epoch")
                || ucd.equals("time.start")
                || ucd.equals("time.end")) {
            meta.addTimesys(new CoordSysMetadata.TimesysDef(TAI_GEO_ID, TAI_TIMESCALE, GEOCENTER_REFPOS));
            return TAI_GEO_ID;
        }
        return null;
    }
}

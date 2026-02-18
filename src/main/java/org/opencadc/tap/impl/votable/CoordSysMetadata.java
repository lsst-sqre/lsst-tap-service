package org.opencadc.tap.impl.votable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Holds coordinate system and time system metadata detected from VOTable field UCDs.
 * Contains definitions for COOSYS and TIMESYS elements to be emitted, and a mapping
 * from field index to the ref ID that should appear on the corresponding FIELD element.
 */
public class CoordSysMetadata {

    /**
     * Represents a VOTable COOSYS definition.
     */
    public static class CoosysDef {
        public final String id;
        public final String system;
        public final String equinox; // null for equinox-independent systems (e.g. ICRS)
        public final String epoch;   // null when not needed

        public CoosysDef(String id, String system) {
            this(id, system, null, null);
        }

        public CoosysDef(String id, String system, String equinox, String epoch) {
            this.id = id;
            this.system = system;
            this.equinox = equinox;
            this.epoch = epoch;
        }
    }

    /**
     * Represents a VOTable TIMESYS definition.
     */
    public static class TimesysDef {
        public final String id;
        public final String timescale;
        public final String refposition;

        public TimesysDef(String id, String timescale, String refposition) {
            this.id = id;
            this.timescale = timescale;
            this.refposition = refposition;
        }
    }

    // LinkedHashMap preserves insertion order, so elements are emitted in a stable sequence.
    private final Map<String, CoosysDef> coosysDefs = new LinkedHashMap<>();
    private final Map<String, TimesysDef> timesysDefs = new LinkedHashMap<>();
    // field index (0-based) → ref ID
    private final Map<Integer, String> fieldRefs = new LinkedHashMap<>();

    /**
     * Add a COOSYS definition. Idempotent: if a definition with the same ID already
     * exists the call has no effect.
     */
    public void addCoosys(CoosysDef def) {
        coosysDefs.putIfAbsent(def.id, def);
    }

    /**
     * Add a TIMESYS definition. Idempotent: if a definition with the same ID already
     * exists the call has no effect.
     */
    public void addTimesys(TimesysDef def) {
        timesysDefs.putIfAbsent(def.id, def);
    }

    /**
     * Record that the field at the given index should reference the given element ID.
     */
    public void setFieldRef(int fieldIndex, String refId) {
        fieldRefs.put(fieldIndex, refId);
    }

    public Map<String, CoosysDef> getCoosysDefs() {
        return Collections.unmodifiableMap(coosysDefs);
    }

    public Map<String, TimesysDef> getTimesysDefs() {
        return Collections.unmodifiableMap(timesysDefs);
    }

    public Map<Integer, String> getFieldRefs() {
        return Collections.unmodifiableMap(fieldRefs);
    }

    /** Returns true when there are no COOSYS or TIMESYS definitions to emit. */
    public boolean isEmpty() {
        return coosysDefs.isEmpty() && timesysDefs.isEmpty();
    }
}

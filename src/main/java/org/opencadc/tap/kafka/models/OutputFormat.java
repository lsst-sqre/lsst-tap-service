package org.opencadc.tap.kafka.models;

/**
 * Simple model for output format.
 */
public final class OutputFormat {
    private final String name;
    private final String mimeType;
    private final String extension;
    private final String serialization;

    public static final OutputFormat VOTABLE =
        new OutputFormat("VOTable", "application/x-votable+xml", "xml", "BINARY2");

    public static final OutputFormat PARQUET =
        new OutputFormat("Parquet", "application/vnd.apache.parquet", "parquet", null);

    private OutputFormat(String name, String mimeType, String extension, String serialization) {
        this.name = name;
        this.mimeType = mimeType;
        this.extension = extension;
        this.serialization = serialization;
    }

    public String getName() {
        return name;
    }

    public String getMimeType() {
        return mimeType;
    }

    public String getExtension() {
        return extension;
    }

    public String getSerialization() {
        return serialization;
    }

    @Override
    public String toString() {
        return name + " (" + mimeType + ")";
    }

    /**
     * Resolve an OutputFormat from a string value.
     * Defaults to VOTABLE if unknown.
     */
    public static OutputFormat fromString(String value) {
        if (value == null) {
            return VOTABLE;
        }
        switch (value.trim().toUpperCase()) {
            case "PARQUET":
            case "APPLICATION/VND.APACHE.PARQUET":
                return PARQUET;
            case "VOTABLE":
            case "APPLICATION/X-VOTABLE+XML":
            default:
                return VOTABLE;
        }
    }
}

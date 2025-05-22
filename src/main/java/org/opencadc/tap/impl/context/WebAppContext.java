package org.opencadc.tap.impl.context;

import javax.servlet.ServletContext;
import org.apache.log4j.Logger;

/**
 * Singleton class that holds the ServletContext and provides application-wide
 * access to it
 * 
 * @author stvoutsin
 */
public class WebAppContext {
    private static final Logger log = Logger.getLogger(WebAppContext.class);

    private static ServletContext servletContext;

    /**
     * Set the ServletContext for the application
     * 
     * @param context The ServletContext to store
     */
    public static void setServletContext(ServletContext context) {
        if (context != null) {
            log.info("Storing ServletContext in WebAppContext singleton");
            servletContext = context;
        }
    }

    /**
     * Get the stored ServletContext
     * 
     * @return The stored ServletContext, or null if not set
     */
    public static ServletContext getServletContext() {
        return servletContext;
    }

    /**
     * Retrieve an attribute from the ServletContext
     * 
     * @param attributeName The name of the attribute to retrieve
     * @return The attribute value, or null if not found or ServletContext is not
     *         set
     */
    public static Object getContextAttribute(String attributeName) {
        if (servletContext == null) {
            log.warn("ServletContext not set in WebAppContext");
            return null;
        }

        Object attribute = servletContext.getAttribute(attributeName);
        if (attribute == null) {
            log.warn("Attribute '" + attributeName + "' not found in ServletContext");
        }

        return attribute;
    }
}
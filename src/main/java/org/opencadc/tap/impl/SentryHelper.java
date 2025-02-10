package org.opencadc.tap.impl;

import io.sentry.ITransaction;
import io.sentry.TransactionOptions;
import io.sentry.Sentry;
import io.sentry.SpanStatus;
import io.sentry.ISpan;
import io.sentry.TransactionOptions;

/**
 * Helper class to manage Sentry operations.
 * 
 * Checks SENTRY_DSN env variable and if creates spans only if enabled
 *  
 */
public class SentryHelper {
    private static final String SENTRY_DSN_ENV = "SENTRY_DSN";
    private static Boolean isSentryEnabled = null;

    /**
     * Checks if Sentry is enabled by looking for SENTRY_DSN environment variable
     * 
     * @return true if Sentry is enabled otherwise false
     */
    public static boolean isSentryEnabled() {
        if (isSentryEnabled == null) {
            String dsn = System.getenv(SENTRY_DSN_ENV);
            isSentryEnabled = dsn != null && !dsn.trim().isEmpty();
        }
        return isSentryEnabled;
    }
    
    /**
     * Gets the current active span from Sentry
     * 
     * @return The current span if Sentry is enabled, null otherwise
     */
    public static ISpan getSpan() {
        if (!isSentryEnabled()) {
            return null;
        }
        return Sentry.getSpan();
    }

    /**
     * Starts a Sentry transaction (if Sentry enabled)
     * 
     * @param operation The operation name
     * @param type The transaction type
     * @return The transaction if Sentry is enabled, null otherwise
     */
    public static ITransaction startTransaction(String operation, String type) {
        if (!isSentryEnabled()) {
            return null;
        }
        TransactionOptions txOptions = new TransactionOptions();
        txOptions.setBindToScope(true);
        return Sentry.startTransaction(operation, type, txOptions);
    }

    /**
     * Finish the transaction if it exists
     * 
     * @param transaction The transaction to finish
     */
    public static void finishTransaction(ITransaction transaction) {
        if (transaction != null) {
            transaction.finish();
        }
    }
}
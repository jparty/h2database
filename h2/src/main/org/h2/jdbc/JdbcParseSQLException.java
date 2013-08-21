/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import org.h2.api.SQLParseException;

import java.util.List;

/**
 * A JDBC SQL Exception with additional parameters.
 * @author Nicolas Fortin
 */
public class JdbcParseSQLException extends JdbcSQLException implements SQLParseException {
    private List<String> expectedTokens;
    private int syntaxErrorPosition;

    /**
     * Creates a SQLException.
     *
     * @param message the reason
     * @param sql the SQL statement
     * @param state the SQL state
     * @param errorCode the error code
     * @param cause the exception that was the reason for this exception
     * @param stackTrace the stack trace
     * @param expectedTokens H2 parser expected tokens
     * @param syntaxErrorPosition Syntax error character index
     */
    public JdbcParseSQLException(String message, String sql, String state, int errorCode, Throwable cause, String stackTrace, List<String> expectedTokens, int syntaxErrorPosition) {
        super(message, sql, state, errorCode, cause, stackTrace);
        this.expectedTokens = expectedTokens;
        this.syntaxErrorPosition = syntaxErrorPosition;
    }

    @Override
    public List<String> getExpectedTokens() {
        return expectedTokens;
    }

    @Override
    public int getSyntaxErrorPosition() {
        return syntaxErrorPosition;
    }
}

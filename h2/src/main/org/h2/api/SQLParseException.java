/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

import java.util.List;

/**
 * H2 SQLException provide the syntax error position and expected token.
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public interface SQLParseException {
    /**
     * @return H2 raise an exception because one of the following token is missing.
     */
    List<String> getExpectedTokens();

    /**
     * @return Syntax error position.
     */
    int getSyntaxErrorPosition();
}

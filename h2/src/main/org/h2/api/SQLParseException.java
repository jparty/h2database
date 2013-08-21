package org.h2.api;

import java.util.List;

/**
 * H2 SQLException provide the syntax error position and expected token.
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public interface SQLParseException {
    /**
     * @return
     */
    List<String> getExpectedTokens();
    int getSyntaxErrorPosition();
}

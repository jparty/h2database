/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import org.h2.bnf.Bnf;
import org.h2.bnf.context.DbContents;
import org.h2.bnf.context.DbContextRule;
import org.h2.bnf.context.DbProcedure;
import org.h2.bnf.context.DbSchema;
import org.h2.test.TestBase;

import java.sql.Connection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Test Bnf Sql parser
 * @author Nicolas Fortin
 */
public class TestBnf extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        deleteDb("TestBnf");
        Connection conn = getConnection("TestBnf");
        try {
            testProcedures(conn);
        } finally {
            conn.close();
        }
    }

    private void testProcedures(Connection conn) throws Exception {
        // Register a procedure and check if it is present in DbContents
        conn.createStatement().execute("DROP ALIAS IF EXISTS CUSTOM_PRINT");
        conn.createStatement().execute("CREATE ALIAS CUSTOM_PRINT AS $$ void print(String s) { System.out.println(s); } $$");
        conn.createStatement().execute("DROP TABLE IF EXISTS TABLE_WITH_STRING_FIELD");
        conn.createStatement().execute("CREATE TABLE TABLE_WITH_STRING_FIELD (STRING_FIELD VARCHAR(50), INT_FIELD integer)");
        DbContents dbContents = new DbContents();
        dbContents.readContents(conn.getMetaData());
        DbSchema defaultSchema = dbContents.getDefaultSchema();
        DbProcedure[] procedures = defaultSchema.getProcedures();
        Set<String> procedureName = new HashSet<String>(procedures.length);
        for(DbProcedure procedure : procedures) {
            procedureName.add(procedure.getName());
        }
        assertTrue(procedureName.contains("CUSTOM_PRINT"));

        // Test completion
        Bnf bnf = Bnf.getInstance(null);
        DbContextRule columnRule = new DbContextRule(dbContents, DbContextRule.COLUMN);
        bnf.updateTopic("column_name", columnRule);
        bnf.updateTopic("expression", new DbContextRule(dbContents, DbContextRule.PROCEDURE));
        bnf.linkStatements();
        // Test partial
        Map<String, String> tokens = bnf.getNextTokenList("SELECT CUSTOM_PR");
        assertTrue(tokens.values().contains("INT"));
        // Test parameters
        tokens = bnf.getNextTokenList("SELECT CUSTOM_PRINT(");
        assertTrue(tokens.values().contains("STRING_FIELD"));
        assertFalse(tokens.values().contains("INT_FIELD"));

        // Test parameters with spaces
        tokens = bnf.getNextTokenList("SELECT CUSTOM_PRINT ( ");
        assertTrue(tokens.values().contains("STRING_FIELD"));
        assertFalse(tokens.values().contains("INT_FIELD"));

        // Test parameters with close bracket
        tokens = bnf.getNextTokenList("SELECT CUSTOM_PRINT ( STRING_FIELD");
        assertTrue(tokens.values().contains(")"));
    }
}
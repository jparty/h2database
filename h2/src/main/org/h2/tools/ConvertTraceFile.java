/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.h2.message.Message;
import org.h2.util.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;
import org.h2.util.Tool;

/**
 * Convert a trace file to a java class.
 * This is required because the find command truncates lines.
 */
public class ConvertTraceFile extends Tool {
    
    private HashMap stats = new HashMap();
    private long timeTotal;
    
    private static class Stat implements Comparable {
        String sql;
        int executeCount;
        long time;
        long resultCountTotal;
        
        public int compareTo(Object o) {
            Stat other = (Stat) o;
            if (other == this) {
                return 0;
            }
            int c = other.time > time ? 1 : other.time < time ? -1 : 0;
            if (c == 0) {
                c = other.executeCount > executeCount ? 1 : other.executeCount < executeCount ? -1 : 0;
                if (c == 0) {
                    c = sql.compareTo(other.sql);
                }
            }
            return c;
        }
    }

    private void showUsage() {
        out.println("Converts a .trace.db file to a SQL script and Java source code.");
        out.println("java "+getClass().getName() + "\n" +
                " [-traceFile <file>]  The trace file name (default: test.trace.db)\n" +
                " [-script <file>]     The script file name (default: test.sql)\n" +
                " [-javaClass <file>]  The Java directory and class file name (default: Test)");
        out.println("See also http://h2database.com/javadoc/" + getClass().getName().replace('.', '/') + ".html");
    }

    /**
     * The command line interface for this tool. The options must be split into
     * strings like this: "-traceFile", "test.trace.db",... Options are case
     * sensitive. The following options are supported:
     * <ul>
     * <li>-help or -? (print the list of options) </li>
     * <li>-traceFile filename (the default is test.trace.db) </li>
     * <li>-script filename (the default is test.sql) </li>
     * <li>-javaClass className (the default is Test) </li>
     * </ul>
     * 
     * @param args the command line arguments
     * @throws Exception
     */
    public static void main(String[] args) throws SQLException {
        new ConvertTraceFile().run(args);
    }

    public void run(String[] args) throws SQLException {
        String traceFile = "test.trace.db";
        String javaClass = "Test";
        String script = "test.sql";
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-traceFile")) {
                traceFile = args[++i];
            } else if (arg.equals("-javaClass")) {
                javaClass = args[++i];
            } else if (arg.equals("-script")) {
                script = args[++i];
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else {
                out.println("Unsupported option: " + arg);
                showUsage();
                return;
            }
        }
        try {
            convertFile(traceFile, javaClass, script);
        } catch (IOException e) {
            throw Message.convertIOException(e, traceFile);
        }
    }

    /**
     * Converts a trace file to a Java class file and a script file.
     *
     * @param traceFileName
     * @param javaClassName
     * @throws IOException
     */
    private void convertFile(String traceFileName, String javaClassName, String script) throws IOException, SQLException {
        LineNumberReader reader = new LineNumberReader(IOUtils.getReader(FileUtils.openFileInputStream(traceFileName)));
        PrintWriter javaWriter = new PrintWriter(FileUtils.openFileWriter(javaClassName + ".java", false));
        PrintWriter scriptWriter = new PrintWriter(FileUtils.openFileWriter(script, false));
        javaWriter.println("import java.io.*;");
        javaWriter.println("import java.sql.*;");
        javaWriter.println("import java.math.*;");
        javaWriter.println("import java.util.Calendar;");
        String cn = javaClassName.replace('\\', '/');
        int idx = cn.lastIndexOf('/');
        if (idx > 0) {
            cn = cn.substring(idx + 1);
        }
        javaWriter.println("public class " + cn + " {");
        javaWriter.println("    public static void main(String[] args) throws Exception {");
        javaWriter.println("        Class.forName(\"org.h2.Driver\");");
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            if (line.startsWith("/**/")) {
                line = "        " + line.substring(4);
                javaWriter.println(line);
            } else if (line.startsWith("/*SQL")) {
                int end = line.indexOf("*/");
                String sql = line.substring(end + "*/".length());
                line = line.substring("/*SQL".length(), end);
                if (line.length() > 0) {
                    int len = sql.length();
                    int count = 0;
                    int time = 0;
                    line = line.trim();
                    if (line.length() > 0) {
                        StringTokenizer tk = new StringTokenizer(line, " :");
                        while (tk.hasMoreElements()) {
                            String token = tk.nextToken();
                            if ("l".equals(token)) {
                                len = Integer.parseInt(tk.nextToken());
                            } else if ("#".equals(token)) {
                                count = Integer.parseInt(tk.nextToken());
                            } else if ("t".equals(token)) {
                                time = Integer.parseInt(tk.nextToken());
                            }
                        }
                    }
                    String statement = sql.substring(0, len);
                    addToStats(statement, count, time);
                }
                scriptWriter.println(StringUtils.javaDecode(sql));
            }
        }
        javaWriter.println("    }");
        javaWriter.println("}");
        reader.close();
        javaWriter.close();
        if (stats.size() > 0) {
            scriptWriter.println("---------------------------------------------------------------");
            ArrayList list = new ArrayList(stats.values());
            Collections.sort(list);
            
            int todo;
            scriptWriter.println("--");
        }
        scriptWriter.close();
    }

    private void addToStats(String sql, int resultCount, int time) {
        Stat stat = (Stat) stats.get(sql);
        if (stat == null) {
            stat = new Stat();
            stat.sql = sql;
            stats.put(sql, stat);
        }
        stat.executeCount++;
        stat.resultCountTotal += resultCount;
        stat.time += time;
        timeTotal += time;
    }

}

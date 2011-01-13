/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
//## Java 1.6 begin ##
import java.sql.NClob;
import java.sql.SQLXML;
import java.sql.RowId;
//## Java 1.6 end ##
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import org.h2.constant.ErrorCode;
import org.h2.expression.ParameterInterface;
import org.h2.message.DbException;
import org.h2.message.TraceObject;
import org.h2.util.BitField;
import org.h2.util.New;
import org.h2.value.ValueNull;

/**
 * Represents a callable statement.
 *
 * @author Sergi Vladykin
 * @author Thomas Mueller
 */
public class JdbcCallableStatement extends JdbcPreparedStatement implements CallableStatement {

    private BitField outParameters;
    private int maxOutParameters;
    private HashMap<String, Integer> namedParameters;

    JdbcCallableStatement(JdbcConnection conn, String sql, int id, int resultSetType, int resultSetConcurrency) {
        super(conn, sql, id, resultSetType, resultSetConcurrency, false);
        setTrace(session.getTrace(), TraceObject.CALLABLE_STATEMENT, id);
    }

    /**
     * Executes an arbitrary statement. If another result set exists for this
     * statement, this will be closed (even if this statement fails). If auto
     * commit is on, and the statement is not a select, this statement will be
     * committed.
     *
     * @return true if a result set is available, false if not
     * @throws SQLException if this object is closed or invalid
     */
    public boolean execute() throws SQLException {
        try {
            checkClosed();
            if (command.isQuery()) {
                super.executeQuery().next();
                return true;
            }
            super.executeUpdate();
            return false;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement (insert, update, delete, create, drop)
     * and returns the update count.
     * If another result set exists for this statement, this will be closed
     * (even if this statement fails).
     *
     * If auto commit is on, this statement will be committed.
     * If the statement is a DDL statement (create, drop, alter) and does not
     * throw an exception, the current transaction (if any) is committed after
     * executing the statement.
     *
     * @return the update count (number of row affected by an insert, update or
     *         delete, or 0 if no rows or the statement was a create, drop,
     *         commit or rollback)
     * @throws SQLException if this object is closed or invalid
     */
    public int executeUpdate() throws SQLException {
        try {
            checkClosed();
            if (command.isQuery()) {
                super.executeQuery().next();
                return 0;
            }
            return super.executeUpdate();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Registers the given OUT parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param sqlType the data type (Types.x) - ignored
     */
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        registerOutParameter(parameterIndex);
    }

    /**
     * Registers the given OUT parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param sqlType the data type (Types.x) - ignored
     * @param typeName the SQL type name - ignored
     */
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        registerOutParameter(parameterIndex);
    }

    /**
     * Registers the given OUT parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param sqlType the data type (Types.x)
     * @param scale is ignored
     */
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        registerOutParameter(parameterIndex);
    }

    /**
     * Registers the given OUT parameter.
     *
     * @param parameterName the parameter name
     * @param sqlType the data type (Types.x) - ignored
     * @param typeName the SQL type name - ignored
     */
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        registerOutParameter(getIndexForName(parameterName), sqlType, typeName);
    }

    /**
     * Registers the given OUT parameter.
     *
     * @param parameterName the parameter name
     * @param sqlType the data type (Types.x) - ignored
     * @param scale is ignored
     */
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        registerOutParameter(getIndexForName(parameterName), sqlType, scale);
    }

    /**
     * Registers the given OUT parameter.
     *
     * @param parameterName the parameter name
     * @param sqlType the data type (Types.x) - ignored
     */
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        registerOutParameter(getIndexForName(parameterName), sqlType);
    }

    /**
     * Returns whether the last column accessed was null.
     *
     * @return true if the last column accessed was null
     */
    public boolean wasNull() throws SQLException {
        return getOpenResultSet().wasNull();
    }

    /**
     * [Not supported]
     */
    public URL getURL(int parameterIndex) throws SQLException {
        throw unsupported("url");
    }

    /**
     * Returns the value of the specified column as a String.
     *
     * @param parameterIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public String getString(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getString(parameterIndex);
    }

    /**
     * Returns the value of the specified column as a boolean.
     *
     * @param parameterIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public boolean getBoolean(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getBoolean(parameterIndex);
    }

    /**
     * Returns the value of the specified column as a byte.
     *
     * @param parameterIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public byte getByte(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getByte(parameterIndex);
    }

    /**
     * Returns the value of the specified column as a short.
     *
     * @param parameterIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public short getShort(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getShort(parameterIndex);
    }

    /**
     * Returns the value of the specified column as an int.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public int getInt(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getInt(parameterIndex);
    }

    /**
     * Returns the value of the specified column as a long.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public long getLong(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getLong(parameterIndex);
    }

    /**
     * Returns the value of the specified column as a float.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public float getFloat(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getFloat(parameterIndex);
    }

    /**
     * Returns the value of the specified column as a double.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public double getDouble(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getDouble(parameterIndex);
    }

    /**
     * Returns the value of the specified column as a BigDecimal.
     *
     * @deprecated
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getBigDecimal(parameterIndex, scale);
    }

    /**
     * Returns the value of the specified column as a byte array.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public byte[] getBytes(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getBytes(parameterIndex);
    }

    /**
     * Returns the value of the specified column as a java.sql.Date.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Date getDate(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getDate(parameterIndex);
    }

    /**
     * Returns the value of the specified column as a java.sql.Time.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Time getTime(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getTime(parameterIndex);
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getTimestamp(parameterIndex);
    }

    /**
     * Returns a column value as a Java object. The data is
     * de-serialized into a Java object (on the client side).
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @return the value or null
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Object getObject(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getObject(parameterIndex);
    }

    /**
     * Returns the value of the specified column as a BigDecimal.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getBigDecimal(parameterIndex);
    }

    /**
     * [Not supported] Gets a column as a object using the specified type
     * mapping.
     */
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        throw unsupported("map");
    }

    /**
     * [Not supported] Gets a column as a reference.
     */
    public Ref getRef(int parameterIndex) throws SQLException {
        throw unsupported("ref");
    }

    /**
     * Returns the value of the specified column as a Blob.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Blob getBlob(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getBlob(parameterIndex);
    }

    /**
     * Returns the value of the specified column as a Clob.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Clob getClob(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getClob(parameterIndex);
    }

    /**
     * Returns the value of the specified column as an Array.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Array getArray(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getArray(parameterIndex);
    }

    /**
     * Returns the value of the specified column as a java.sql.Date using a
     * specified time zone.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param cal the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getDate(parameterIndex, cal);
    }

    /**
     * Returns the value of the specified column as a java.sql.Time using a
     * specified time zone.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param cal the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getTime(parameterIndex, cal);
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp using a
     * specified time zone.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param cal the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getTimestamp(parameterIndex, cal);
    }

    /**
     * [Not supported]
     */
    public URL getURL(String parameterName) throws SQLException {
        throw unsupported("url");
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp using a
     * specified time zone.
     *
     * @param parameterName the parameter name
     * @param cal the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return getTimestamp(getIndexForName(parameterName), cal);
    }

    /**
     * Returns the value of the specified column as a java.sql.Time using a
     * specified time zone.
     *
     * @param parameterName the parameter name
     * @param cal the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return getTime(getIndexForName(parameterName), cal);
    }

    /**
     * Returns the value of the specified column as a java.sql.Date using a
     * specified time zone.
     *
     * @param parameterName the parameter name
     * @param cal the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return getDate(getIndexForName(parameterName), cal);
    }

    /**
     * Returns the value of the specified column as an Array.
     *
     * @param parameterName the parameter name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Array getArray(String parameterName) throws SQLException {
        return getArray(getIndexForName(parameterName));
    }

    /**
     * Returns the value of the specified column as a Clob.
     *
     * @param parameterName the parameter name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Clob getClob(String parameterName) throws SQLException {
        return getClob(getIndexForName(parameterName));
    }

    /**
     * Returns the value of the specified column as a Blob.
     *
     * @param parameterName the parameter name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Blob getBlob(String parameterName) throws SQLException {
        return getBlob(getIndexForName(parameterName));
    }

    /**
     * [Not supported] Gets a column as a reference.
     */
    public Ref getRef(String parameterName) throws SQLException {
        throw unsupported("ref");
    }

    /**
     * [Not supported] Gets a column as a object using the specified type
     * mapping.
     */
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        throw unsupported("map");
    }

    /**
     * Returns the value of the specified column as a BigDecimal.
     *
     * @param parameterName the parameter name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return getBigDecimal(getIndexForName(parameterName));
    }

    /**
     * Returns a column value as a Java object. The data is
     * de-serialized into a Java object (on the client side).
     *
     * @param parameterName the parameter name
     * @return the value or null
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Object getObject(String parameterName) throws SQLException {
        return getObject(getIndexForName(parameterName));
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp.
     *
     * @param parameterName the parameter name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return getTimestamp(getIndexForName(parameterName));
    }

    /**
     * Returns the value of the specified column as a java.sql.Time.
     *
     * @param parameterName the parameter name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Time getTime(String parameterName) throws SQLException {
        return getTime(getIndexForName(parameterName));
    }

    /**
     * Returns the value of the specified column as a java.sql.Date.
     *
     * @param parameterName the parameter name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Date getDate(String parameterName) throws SQLException {
        return getDate(getIndexForName(parameterName));
    }

    /**
     * Returns the value of the specified column as a byte array.
     *
     * @param parameterName the parameter name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public byte[] getBytes(String parameterName) throws SQLException {
        return getBytes(getIndexForName(parameterName));
    }

    /**
     * Returns the value of the specified column as a double.
     *
     * @param parameterName the parameter name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public double getDouble(String parameterName) throws SQLException {
        return getDouble(getIndexForName(parameterName));
    }

    /**
     * Returns the value of the specified column as a float.
     *
     * @param parameterName the parameter name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public float getFloat(String parameterName) throws SQLException {
        return getFloat(getIndexForName(parameterName));
    }

    /**
     * Returns the value of the specified column as a long.
     *
     * @param parameterName the parameter name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public long getLong(String parameterName) throws SQLException {
        return getLong(getIndexForName(parameterName));
    }

    /**
     * Returns the value of the specified column as an int.
     *
     * @param parameterName the parameter name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public int getInt(String parameterName) throws SQLException {
        return getInt(getIndexForName(parameterName));
    }

    /**
     * Returns the value of the specified column as a short.
     *
     * @param parameterName the parameter name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public short getShort(String parameterName) throws SQLException {
        return getShort(getIndexForName(parameterName));
    }

    /**
     * Returns the value of the specified column as a byte.
     *
     * @param parameterName the parameter name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public byte getByte(String parameterName) throws SQLException {
        return getByte(getIndexForName(parameterName));
    }

    /**
     * Returns the value of the specified column as a boolean.
     *
     * @param parameterName the parameter name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public boolean getBoolean(String parameterName) throws SQLException {
        return getBoolean(getIndexForName(parameterName));
    }

    /**
     * Returns the value of the specified column as a String.
     *
     * @param parameterName the parameter name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public String getString(String parameterName) throws SQLException {
        return getString(getIndexForName(parameterName));
    }

    /**
     * [Not supported] Returns the value of the specified column as a row id.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     */
//## Java 1.6 begin ##
    public RowId getRowId(int parameterIndex) throws SQLException {
        throw unsupported("rowId");
    }
//## Java 1.6 end ##

    /**
     * [Not supported] Returns the value of the specified column as a row id.
     *
     * @param parameterName the parameter name
     */
//## Java 1.6 begin ##
    public RowId getRowId(String parameterName) throws SQLException {
        throw unsupported("rowId");
    }
//## Java 1.6 end ##

    /**
     * Returns the value of the specified column as a Clob.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
//## Java 1.6 begin ##
    public NClob getNClob(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getNClob(parameterIndex);
    }
//## Java 1.6 end ##

    /**
     * Returns the value of the specified column as a Clob.
     *
     * @param parameterName the parameter name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
//## Java 1.6 begin ##
    public NClob getNClob(String parameterName) throws SQLException {
        return getNClob(getIndexForName(parameterName));
    }
//## Java 1.6 end ##

    /**
     * [Not supported] Returns the value of the specified column as a SQLXML object.
     */
//## Java 1.6 begin ##
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        throw unsupported("SQLXML");
    }
//## Java 1.6 end ##

    /**
     * [Not supported] Returns the value of the specified column as a SQLXML object.
     */
//## Java 1.6 begin ##
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        throw unsupported("SQLXML");
    }
//## Java 1.6 end ##

    /**
     * Returns the value of the specified column as a String.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
//## Java 1.6 begin ##
    public String getNString(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getNString(parameterIndex);
    }
//## Java 1.6 end ##

    /**
     * Returns the value of the specified column as a String.
     *
     * @param parameterName the parameter name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
//## Java 1.6 begin ##
    public String getNString(String parameterName) throws SQLException {
        return getNString(getIndexForName(parameterName));
    }
//## Java 1.6 end ##

    /**
     * Returns the value of the specified column as a reader.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
//## Java 1.6 begin ##
    public Reader getNCharacterStream(int parameterIndex)
            throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getNCharacterStream(parameterIndex);
    }
//## Java 1.6 end ##

    /**
     * Returns the value of the specified column as a reader.
     *
     * @param parameterName the parameter name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
//## Java 1.6 begin ##
    public Reader getNCharacterStream(String parameterName)
            throws SQLException {
        return getNCharacterStream(getIndexForName(parameterName));
    }
//## Java 1.6 end ##

    /**
     * Returns the value of the specified column as a reader.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
//## Java 1.6 begin ##
    public Reader getCharacterStream(int parameterIndex)
            throws SQLException {
        checkRegistered(parameterIndex);
        return getOpenResultSet().getCharacterStream(parameterIndex);
    }
//## Java 1.6 end ##

    /**
     * Returns the value of the specified column as a reader.
     *
     * @param parameterName the parameter name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
//## Java 1.6 begin ##
    public Reader getCharacterStream(String parameterName)
            throws SQLException {
        return getCharacterStream(getIndexForName(parameterName));
    }
//## Java 1.6 end ##

    // =============================================================

    /**
     * Sets a parameter to null.
     *
     * @param parameterName the parameter name
     * @param sqlType the data type (Types.x)
     * @param typeName this parameter is ignored
     * @throws SQLException if this object is closed
     */
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        setNull(getIndexForName(parameterName), sqlType, typeName);
    }

    /**
     * Sets a parameter to null.
     *
     * @param parameterName the parameter name
     * @param sqlType the data type (Types.x)
     * @throws SQLException if this object is closed
     */
    public void setNull(String parameterName, int sqlType) throws SQLException {
        setNull(getIndexForName(parameterName), sqlType);
    }

    /**
     * Sets the timestamp using a specified time zone. The value will be
     * converted to the local time zone.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @param cal the calendar
     * @throws SQLException if this object is closed
     */
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(getIndexForName(parameterName), x, cal);
    }

    /**
     * Sets the time using a specified time zone. The value will be converted to
     * the local time zone.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @param cal the calendar
     * @throws SQLException if this object is closed
     */
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        setTime(getIndexForName(parameterName), x, cal);
    }

    /**
     * Sets the date using a specified time zone. The value will be converted to
     * the local time zone.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @param cal the calendar
     * @throws SQLException if this object is closed
     */
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        setDate(getIndexForName(parameterName), x, cal);
    }

    /**
     * Sets the value of a parameter as a character stream.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @param length the number of bytes
     * @throws SQLException if this object is closed
     */
    public void setCharacterStream(String parameterName, Reader x, int length) throws SQLException {
        setCharacterStream(getIndexForName(parameterName), x, length);
    }

    /**
     * Sets the value of a parameter.
     * Objects of unknown classes are serialized (on the client side).
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setObject(String parameterName, Object x) throws SQLException {
        setObject(getIndexForName(parameterName), x);
    }

    /**
     * Sets the value of a parameter. The object is converted, if required, to
     * the specified data type before sending to the database.
     * Objects of unknown classes are serialized (on the client side).
     *
     * @param parameterName the parameter name
     * @param x the value, null is allowed
     * @param targetSqlType the type as defined in java.sql.Types
     * @throws SQLException if this object is closed
     */
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        setObject(getIndexForName(parameterName), x, targetSqlType);
    }

    /**
     * Sets the value of a parameter. The object is converted, if required, to
     * the specified data type before sending to the database.
     * Objects of unknown classes are serialized (on the client side).
     *
     * @param parameterName the parameter name
     * @param x the value, null is allowed
     * @param targetSqlType the type as defined in java.sql.Types
     * @param scale is ignored
     * @throws SQLException if this object is closed
     */
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        setObject(getIndexForName(parameterName), x, targetSqlType, scale);
    }

    /**
     * Sets the value of a parameter as an input stream.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @param length the number of bytes
     * @throws SQLException if this object is closed
     */
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        setBinaryStream(getIndexForName(parameterName), x, length);
    }

    /**
     * Sets the value of a parameter as an ASCII stream.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @param length the number of bytes
     * @throws SQLException if this object is closed
     */
    public void setAsciiStream(String parameterName,
            InputStream x, long length) throws SQLException {
        setAsciiStream(getIndexForName(parameterName), x, length);
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        setTimestamp(getIndexForName(parameterName), x);
    }

    /**
     * Sets the time using a specified time zone.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setTime(String parameterName, Time x) throws SQLException {
        setTime(getIndexForName(parameterName), x);
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setDate(String parameterName, Date x) throws SQLException {
        setDate(getIndexForName(parameterName), x);
    }

    /**
     * Sets the value of a parameter as a byte array.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        setBytes(getIndexForName(parameterName), x);
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setString(String parameterName, String x) throws SQLException {
        setString(getIndexForName(parameterName), x);
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        setBigDecimal(getIndexForName(parameterName), x);
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setDouble(String parameterName, double x) throws SQLException {
        setDouble(getIndexForName(parameterName), x);
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setFloat(String parameterName, float x) throws SQLException {
        setFloat(getIndexForName(parameterName), x);
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setLong(String parameterName, long x) throws SQLException {
        setLong(getIndexForName(parameterName), x);
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setInt(String parameterName, int x) throws SQLException {
        setInt(getIndexForName(parameterName), x);
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setShort(String parameterName, short x) throws SQLException {
        setShort(getIndexForName(parameterName), x);
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setByte(String parameterName, byte x) throws SQLException {
        setByte(getIndexForName(parameterName), x);
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        setBoolean(getIndexForName(parameterName), x);
    }

    /**
     * [Not supported]
     */
    public void setURL(String parameterName, URL val) throws SQLException {
        throw unsupported("url");
    }

    /**
     * [Not supported] Sets the value of a parameter as a row id.
     */
//## Java 1.6 begin ##
    public void setRowId(String parameterName, RowId x)
            throws SQLException {
        throw unsupported("rowId");
    }
//## Java 1.6 end ##

    /**
     * Sets the value of a parameter.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
//## Java 1.6 begin ##
    public void setNString(String parameterName, String x)
            throws SQLException {
        setNString(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * Sets the value of a parameter as a character stream.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @param length the number of bytes
     * @throws SQLException if this object is closed
     */
//## Java 1.6 begin ##
    public void setNCharacterStream(String parameterName,
            Reader x, long length) throws SQLException {
        setNCharacterStream(getIndexForName(parameterName), x, length);
    }
//## Java 1.6 end ##

    /**
     * Sets the value of a parameter as a Clob.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
//## Java 1.6 begin ##
    public void setNClob(String parameterName, NClob x)
            throws SQLException {
        setNClob(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * Sets the value of a parameter as a Clob.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
//## Java 1.6 begin ##
    public void setClob(String parameterName, Reader x,
            long length) throws SQLException {
        setClob(getIndexForName(parameterName), x, length);
    }
//## Java 1.6 end ##

    /**
     * Sets the value of a parameter as a Blob.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
//## Java 1.6 begin ##
    public void setBlob(String parameterName, InputStream x,
            long length) throws SQLException {
        setBlob(getIndexForName(parameterName), x, length);
    }
//## Java 1.6 end ##

    /**
     * Sets the value of a parameter as a Clob.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
//## Java 1.6 begin ##
    public void setNClob(String parameterName, Reader x,
            long length) throws SQLException {
        setNClob(getIndexForName(parameterName), x, length);
    }
//## Java 1.6 end ##

    /**
     * Sets the value of a parameter as a Blob.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
//## Java 1.6 begin ##
    public void setBlob(String parameterName, Blob x)
            throws SQLException {
        setBlob(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * Sets the value of a parameter as a Clob.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
//## Java 1.6 begin ##
    public void setClob(String parameterName, Clob x) throws SQLException {
        setClob(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * Sets the value of a parameter as an ASCII stream.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
//## Java 1.6 begin ##
    public void setAsciiStream(String parameterName, InputStream x)
            throws SQLException {
        setAsciiStream(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * Sets the value of a parameter as an ASCII stream.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @param length the number of bytes
     * @throws SQLException if this object is closed
     */
    public void setAsciiStream(String parameterName,
            InputStream x, int length) throws SQLException {
        setAsciiStream(getIndexForName(parameterName), x, length);
    }

    /**
     * Sets the value of a parameter as an input stream.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
//## Java 1.6 begin ##
    public void setBinaryStream(String parameterName,
            InputStream x) throws SQLException {
        setBinaryStream(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * Sets the value of a parameter as an input stream.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @param length the number of bytes
     * @throws SQLException if this object is closed
     */
//## Java 1.6 begin ##
    public void setBinaryStream(String parameterName,
            InputStream x, long length) throws SQLException {
        setBinaryStream(getIndexForName(parameterName), x, length);
    }
//## Java 1.6 end ##

    /**
     * Sets the value of a parameter as a Blob.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
//## Java 1.6 begin ##
    public void setBlob(String parameterName, InputStream x)
            throws SQLException {
        setBlob(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * Sets the value of a parameter as a character stream.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
//## Java 1.6 begin ##
    public void setCharacterStream(String parameterName, Reader x)
            throws SQLException {
        setCharacterStream(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * Sets the value of a parameter as a character stream.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if this object is closed
     */
//## Java 1.6 begin ##
    public void setCharacterStream(String parameterName,
            Reader x, long length) throws SQLException {
        setCharacterStream(getIndexForName(parameterName), x, length);
    }
//## Java 1.6 end ##

    /**
     * Sets the value of a parameter as a character stream.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
//## Java 1.6 begin ##
    public void setClob(String parameterName, Reader x) throws SQLException {
        setClob(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * Sets the value of a parameter as a character stream.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
//## Java 1.6 begin ##
    public void setNCharacterStream(String parameterName, Reader x)
            throws SQLException {
        setNCharacterStream(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * Sets the value of a parameter as a Clob.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterName the parameter name
     * @param x the value
     * @throws SQLException if this object is closed
     */
//## Java 1.6 begin ##
    public void setNClob(String parameterName, Reader x)
            throws SQLException {
        setNClob(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * [Not supported] Sets the value of a parameter as a SQLXML object.
     */
//## Java 1.6 begin ##
    public void setSQLXML(String parameterName, SQLXML x)
            throws SQLException {
        throw unsupported("SQLXML");
    }
//## Java 1.6 end ##

    private ResultSetMetaData getCheckedMetaData() throws SQLException {
        ResultSetMetaData meta = getMetaData();
        if (meta == null) {
            throw DbException.getUnsupportedException("Supported only for calling stored procedures");
        }
        return meta;
    }

    private void checkIndexBounds(int parameterIndex) throws SQLException {
        checkClosed();
        if (parameterIndex < 1 || parameterIndex > maxOutParameters) {
            throw DbException.getInvalidValueException("parameterIndex", parameterIndex);
        }
    }

    private void registerOutParameter(int parameterIndex) throws SQLException {
        try {
            checkClosed();
            if (outParameters == null) {
                maxOutParameters = Math.min(
                        getParameterMetaData().getParameterCount(),
                        getCheckedMetaData().getColumnCount());
                outParameters = new BitField();
            }
            checkIndexBounds(parameterIndex);
            ParameterInterface param = command.getParameters().get(--parameterIndex);
            if (param.getParamValue() == null) {
                param.setValue(ValueNull.INSTANCE, false);
            }
            outParameters.set(parameterIndex);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private void checkRegistered(int parameterIndex) throws SQLException {
        try {
            checkIndexBounds(parameterIndex);
            if (!outParameters.get(parameterIndex - 1)) {
                throw DbException.getInvalidValueException("parameterIndex", parameterIndex);
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private int getIndexForName(String parameterName) throws SQLException {
        try {
            checkClosed();
            if (namedParameters == null) {
                ResultSetMetaData meta = getCheckedMetaData();
                int columnCount = meta.getColumnCount();
                HashMap<String, Integer> map = New.hashMap(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    map.put(meta.getColumnLabel(i), i);
                }
                namedParameters = map;
            }
            Integer index = namedParameters.get(parameterName);
            if (index == null) {
                throw DbException.getInvalidValueException("parameterName", parameterName);
            }
            return index;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private JdbcResultSet getOpenResultSet() throws SQLException {
        try {
            checkClosed();
            if (resultSet == null) {
                throw DbException.get(ErrorCode.NO_DATA_AVAILABLE);
            }
            return resultSet;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

}

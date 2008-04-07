/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;
import java.util.HashMap;

import org.h2.command.CommandInterface;
import org.h2.command.Parser;
import org.h2.command.dml.SetTypes;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.util.RandomUtils;
import org.h2.util.StringUtils;

/**
 * The engine contains a map of all open databases.
 * It is also responsible for opening and creating new databases.
 * This is a singleton class.
 */
public class Engine {

    private static final Engine INSTANCE = new Engine();
    private static volatile long wrongPasswordDelay = SysProperties.DELAY_WRONG_PASSWORD_MIN;
    private static Object wrongPasswordSync = new Object();
    private final HashMap databases = new HashMap();

    private Engine() {
        // don't allow others to instantiate
    }

    public static Engine getInstance() {
        return INSTANCE;
    }

    private Session openSession(ConnectionInfo ci, boolean ifExists, String cipher) throws SQLException {
        // may not remove properties here, otherwise they are lost 
        // if it is required to call it twice
        String name = ci.getName();
        Database database;
        if (ci.isUnnamed()) {
            database = null;
        } else {
            database = (Database) databases.get(name);
        }
        User user = null;
        boolean opened = false;
        if (database == null) {
            if (ifExists && !Database.exists(name)) {
                throw Message.getSQLException(ErrorCode.DATABASE_NOT_FOUND_1, name);
            }
            database = new Database(name, ci, cipher);
            opened = true;
            if (database.getAllUsers().size() == 0) {
                // users is the last thing we add, so if no user is around, 
                // the database is not initialized correctly
                user = new User(database, database.allocateObjectId(false, true), ci.getUserName(), false);
                user.setAdmin(true);
                user.setUserPasswordHash(ci.getUserPasswordHash());
                database.setMasterUser(user);
            }
            if (!ci.isUnnamed()) {
                databases.put(name, database);
            }
            database.opened();
        }
        synchronized (database) {
            if (database.isClosing()) {
                return null;
            }
            if (user == null) {
                if (database.validateFilePasswordHash(cipher, ci.getFilePasswordHash())) {
                    user = database.findUser(ci.getUserName());
                    if (user != null) {
                        if (!user.validateUserPasswordHash(ci.getUserPasswordHash())) {
                            user = null;
                        }
                    }
                }
                if (opened && (user == null || !user.getAdmin())) {
                    // reset - because the user is not an admin, and has no
                    // right to listen to exceptions
                    database.setEventListener(null);
                }
            }
            if (user == null) {
                database.removeSession(null);
                throw Message.getSQLException(ErrorCode.WRONG_USER_OR_PASSWORD);
            }
            checkClustering(ci, database);
            Session session = database.createUserSession(user);
            return session;
        }
    }

    public Session getSession(ConnectionInfo ci) throws SQLException {
        try {
            Session session = openSession(ci);
            validateUserAndPassword(true);
            return session;
        } catch (SQLException e) {
            if (e.getErrorCode() == ErrorCode.WRONG_USER_OR_PASSWORD) {
                validateUserAndPassword(false);
            }
            throw e;
        }
    }
    
    private synchronized Session openSession(ConnectionInfo ci) throws SQLException {
        boolean ifExists = ci.removeProperty("IFEXISTS", false);
        boolean ignoreUnknownSetting = ci.removeProperty("IGNORE_UNKNOWN_SETTINGS", false);
        String cipher = ci.removeProperty("CIPHER", null);
        Session session;
        while (true) {
            session = openSession(ci, ifExists, cipher);
            if (session != null) {
                break;
            }
            // we found a database that is currently closing
            // wait a bit to avoid a busy loop (the method is synchronized)
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        String[] keys = ci.getKeys();
        session.setAllowLiterals(true);
        for (int i = 0; i < keys.length; i++) {
            String setting = keys[i];
            String value = ci.getProperty(setting);
            try {
                CommandInterface command = session.prepareCommand("SET " + Parser.quoteIdentifier(setting) + " "
                        + value, Integer.MAX_VALUE);
                command.executeUpdate();
            } catch (SQLException e) {
                if (!ignoreUnknownSetting) {
                    session.close();
                    throw e;
                }
            }
        }
        session.setAllowLiterals(false);
        session.commit(true);
        session.getDatabase().getTrace(Trace.SESSION).info("connected #" + session.getId());
        return session;
    }

    private void checkClustering(ConnectionInfo ci, Database database) throws SQLException {
        String clusterSession = ci.getProperty(SetTypes.CLUSTER, null);
        if (Constants.CLUSTERING_DISABLED.equals(clusterSession)) {
            // in this case, no checking is made
            // (so that a connection can be made to disable/change clustering)
            return;
        }
        String clusterDb = database.getCluster();
        if (!Constants.CLUSTERING_DISABLED.equals(clusterDb)) {
            if (!StringUtils.equals(clusterSession, clusterDb)) {
                if (clusterDb.equals(Constants.CLUSTERING_DISABLED)) {
                    throw Message.getSQLException(ErrorCode.CLUSTER_ERROR_DATABASE_RUNS_ALONE);
                } else {
                    throw Message.getSQLException(ErrorCode.CLUSTER_ERROR_DATABASE_RUNS_CLUSTERED_1, clusterDb);
                }
            }
        }
    }

    public void close(String name) {
        databases.remove(name);
    }
    
    /**
     * This method is called after validating user name and password. If user
     * name and password were correct, the sleep time is reset, otherwise this
     * method waits some time (to make brute force / rainbow table attacks
     * harder) and then throws a 'wrong user or password' exception. The delay
     * is a bit randomized to protect against timing attacks. Also the delay
     * doubles after each unsuccessful logins, to make brute force attacks harder.
     * 
     * There is only one exception both for wrong user and for wrong password,
     * to make it harder to get the list of user names. This method must only be
     * called from one place, so it is not possible from the stack trace to see 
     * if the user name was wrong or the password.
     * 
     * @param correct if the user name or the password was correct
     * @throws SQLException the exception 'wrong user or password'
     */
    private static void validateUserAndPassword(boolean correct) throws SQLException {
        int min = SysProperties.DELAY_WRONG_PASSWORD_MIN;
        if (correct) {
            wrongPasswordDelay = min;
        } else {
            // this method is not synchronized on the Engine, so that
            // successful attempts are not blocked
            synchronized (wrongPasswordSync) {
                long delay = wrongPasswordDelay;
                int max = SysProperties.DELAY_WRONG_PASSWORD_MAX;
                if (max <= 0) {
                    max = Integer.MAX_VALUE;
                }
                wrongPasswordDelay += wrongPasswordDelay;
                if (wrongPasswordDelay > max || wrongPasswordDelay < 0) {
                    wrongPasswordDelay = max;
                }
                if (min > 0) {
                    // a bit more to protect against timing attacks
                    delay += Math.abs(RandomUtils.getSecureLong() % 100);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
                throw Message.getSQLException(ErrorCode.WRONG_USER_OR_PASSWORD);
            }
        }
    }    

}

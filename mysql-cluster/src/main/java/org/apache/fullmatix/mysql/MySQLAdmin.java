package org.apache.fullmatix.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.helix.model.InstanceConfig;
import org.apache.log4j.Logger;

/**
 * All connections created here is used only for admin operations.
 * None of these will be binlogged.
 * Most queries will be single threaded.
 */
public class MySQLAdmin {

  private static final Logger LOG = Logger.getLogger(MySQLAdmin.class);

  Connection _connection;
  private final String _jdbcUrl;
  private final String _user;
  private final String _password;

  public MySQLAdmin(String jdbcurl, String user, String password) {
    _jdbcUrl = jdbcurl;
    _user = user;
    _password = password;
  }

  public MySQLAdmin(InstanceConfig instanceConfig) {
    String mysqlHost = instanceConfig.getRecord().getSimpleField(MySQLConstants.MYSQL_HOST);
    if (mysqlHost == null) {
      // assumes its running on the same node as the agent
      mysqlHost = instanceConfig.getHostName();
    }
    String mysqlPort = instanceConfig.getRecord().getSimpleField(MySQLConstants.MYSQL_PORT);
    _jdbcUrl = "jdbc:mysql://" + mysqlHost + ":" + mysqlPort;
    _user = instanceConfig.getRecord().getSimpleField(MySQLConstants.MYSQL_SUPER_USER);
    _password = instanceConfig.getRecord().getSimpleField(MySQLConstants.MYSQL_SUPER_PASSWORD);
  }

  private void refreshConnection() {
    try {
      _connection = DriverManager.getConnection(_jdbcUrl, _user, _password);
      _connection.createStatement().executeQuery("set sql_log_bin=0");
    } catch (SQLException e) {
      LOG.error("Unable to creation jdbc connection using jdbc url: " + _jdbcUrl, e);
    }
  }

  public Connection getConnection() throws Exception {

    if (_connection == null || !_connection.isValid(100000)) {
      refreshConnection();
    }
    return _connection;

  }

  public synchronized boolean createTable(String database, String table, String tableSpec) {
    try {
      Connection connection = getConnection();
      connection.setCatalog(database);
      String ddl = String.format("create table if not exists %s %s", table, tableSpec);
      LOG.info("Executing create table ddl:" + ddl);
      boolean success = connection.createStatement().execute(ddl);
      return success;
    } catch (Exception e) {
      LOG.error("Exception while creating table", e);
      return false;
    }
  }

  public synchronized boolean createDatabase(String databaseName) {
    String ddl = String.format("create database if not exists %s", databaseName);
    try {
      boolean success = getConnection().createStatement().execute(ddl);
      return success;
    } catch (Exception e) {
      LOG.error("Exception while creating database", e);
      return false;
    }
  }

  public SlaveStatus getSlaveStatus() {
    SlaveStatus slaveStatus = null;
    try {

      ResultSet resultSet = getConnection().createStatement().executeQuery("show slave status");
      slaveStatus = new SlaveStatus(resultSet);
    } catch (Exception e) {
      LOG.error("Exception while executing show slave status", e);
    }
    return slaveStatus;
  }

  public MasterStatus getMasterStatus() {
    MasterStatus masterStatus = null;
    try {

      ResultSet resultSet = getConnection().createStatement().executeQuery("show master status");
      masterStatus = new MasterStatus(resultSet);
    } catch (Exception e) {
      LOG.error("Exception while executing show slave status", e);
    }
    return masterStatus;
  }

  public boolean close() {
    if (_connection != null) {
      try {
        _connection.close();
        return _connection.isClosed();
      } catch (SQLException e) {
        return false;
      }
    }
    return true;
  }

  public boolean ping() {
    Connection connection = null;
    try {
      connection = getConnection();
      return (connection != null) ? connection.isValid(500) : false;
    } catch (Exception e) {
      LOG.warn("Exception while checking if the database connection is healthy", e);
    }
    return false;
  }
}

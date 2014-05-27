package org.apache.fullmatix.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TestReplicator {
  public static void main(String[] args) throws Exception {
    Class.forName("com.mysql.jdbc.Driver");
    String slaveHost = "kgopalak-ld.linkedin.biz";
    String slaveMysqlPort ="5555";
    String userName ="monty";
    String password ="some_pass";
    Connection slaveConnection = DriverManager.getConnection("jdbc:mysql://" + slaveHost + ":" + slaveMysqlPort + "", userName, password);
    slaveConnection.setCatalog("MyDB_0");
    slaveConnection.createStatement().execute("create table MyTable  ( col1 INT, col2 INT ) ");
  }
}

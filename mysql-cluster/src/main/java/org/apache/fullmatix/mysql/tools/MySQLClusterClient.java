package org.apache.fullmatix.mysql.tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.fullmatix.mysql.ConnectionURLProvider;
import org.apache.fullmatix.mysql.MySQLConstants;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.InstanceConfig;
import org.apache.log4j.Logger;

/**
 * Cluster aware mysql client.
 * Given a database, table, partition and a sql, runs the sql on the node that is currently the
 * MASTER for
 * that database partition
 */
public class MySQLClusterClient {

  private static final Logger LOG = Logger.getLogger(MySQLClusterClient.class);

  /**
   * USAGE MySQLClusterClient zkAddress clusterName database partitionId Query
   * @param args
   * @throws Exception
   */
  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {

    Option zkServerOption =
        OptionBuilder.withLongOpt("zkAddress").withDescription("zookeeper address").create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("zookeeperAddress(Required)");
    Option clusterNameOption =
        OptionBuilder.withLongOpt("cluster").withDescription("Cluster Name").create();
    clusterNameOption.setArgs(1);
    clusterNameOption.setRequired(true);
    clusterNameOption.setArgName("clusterName(Required)");
    Option dbNameOption =
        OptionBuilder.withLongOpt("database").withDescription("Database Name").create();
    dbNameOption.setArgs(1);
    dbNameOption.setRequired(true);
    dbNameOption.setArgName("databaseName(Required)");
    Option partitionOption =
        OptionBuilder.withLongOpt("partitionId").withDescription("Partition Id").create();
    partitionOption.setArgs(1);
    partitionOption.setRequired(true);
    partitionOption.setArgName("partitionId(Required)");
    Option queryOption = OptionBuilder.withLongOpt("sql").withDescription("sql").create();
    queryOption.setArgs(1);
    queryOption.setRequired(true);
    queryOption.setArgName("sql(Required)");

    Options options = new Options();
    options.addOption(zkServerOption);
    options.addOption(clusterNameOption);
    options.addOption(dbNameOption);
    options.addOption(partitionOption);
    options.addOption(queryOption);
    CommandLine cliParser = new GnuParser().parse(options, args);
    String zkAddress = cliParser.getOptionValue("zkAddress");
    String cluster = cliParser.getOptionValue("cluster");
    String database = cliParser.getOptionValue("database");
    String table = cliParser.getOptionValue("database");
    int partitionId = Integer.parseInt(cliParser.getOptionValue("partitionId"));
    String sql = cliParser.getOptionValue("sql");

    ConnectionURLProvider connectionURLProvider = new ConnectionURLProvider(zkAddress, cluster);
    connectionURLProvider.start();
    InstanceConfig instanceConfig = connectionURLProvider.getMaster(database, partitionId);
    String host = instanceConfig.getHostName();
    ZNRecord record = instanceConfig.getRecord();
    String port = record.getSimpleField(MySQLConstants.MYSQL_PORT);
    String userName = record.getSimpleField(MySQLConstants.MYSQL_SUPER_USER);
    String password = record.getSimpleField(MySQLConstants.MYSQL_SUPER_PASSWORD);
    Connection connection =
        DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "", userName, password);
    connection.setAutoCommit(true);
    connection.setCatalog(database +"_"+ partitionId);
    Statement statement = connection.createStatement();
    boolean execute = statement.execute(sql);
    if (execute) {
      ResultSet rs = statement.getResultSet();
      ResultSetMetaData rsmd = rs.getMetaData();
      int columnsNumber = rsmd.getColumnCount();
      StringBuilder sb = new StringBuilder();
      while (rs.next()) {
        for (int i = 1; i <= columnsNumber; i++) {
          if (i > 1)
            sb.append(",  ");
          String columnValue = rs.getString(i);
          sb.append(rsmd.getColumnName(i)+ " " + columnValue );
        }
        sb.append("\n");
      }
      LOG.info(sb);
    } else {
      LOG.info("Executed sql:" + sql + " on " + instanceConfig.getInstanceName());
    }
    connectionURLProvider.stop();
  }
}

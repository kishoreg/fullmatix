package org.apache.fullmatix.mysql.tools;

import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.fullmatix.mysql.MySQLConstants;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.HelixProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class LoadGenerator implements Runnable
{

  private static final Logger LOG = Logger.getLogger(LoadGenerator.class);
  private String _zkAddress;
  private String _cluster;
  private boolean _shudPause = false;
  private boolean _stop = false;
  Lock lock = new ReentrantLock();
  Condition paused = lock.newCondition();
  Condition run = lock.newCondition();
  Condition running = lock.newCondition();

  public LoadGenerator(String zkAddress, String cluster)
  {
    _zkAddress = zkAddress;
    _cluster = cluster;
  }

  public void pause() throws InterruptedException
  {
    _shudPause = true;
    lock.lock();
    try
    {
      paused.await();
    } finally
    {
      lock.unlock();
    }
  }

  public void unpause() throws InterruptedException
  {
    _shudPause = false;
    lock.lock();
    try
    {
      run.signal();
      running.await();
    } finally
    {
      lock.unlock();
    }
  }

  public void stop()
  {
    _stop = true;
  }

  @Override
  public void run()
  {
    try
    {
      HelixManager manager = HelixManagerFactory.getZKHelixManager(_cluster,
          "loadgenerator", InstanceType.SPECTATOR, _zkAddress);
      manager.connect();
      RoutingTableProvider routingTableProvider = new RoutingTableProvider();
      manager.addExternalViewChangeListener(routingTableProvider);
      Class.forName("com.mysql.jdbc.Driver");

      List<String> databases = new ArrayList<String>();
      Map<String, List<String>> databaseTablesMap = new HashMap<String, List<String>>();
      PropertyKey key = new PropertyKey.Builder(_cluster).resourceConfigs();
      Map<String, HelixProperty> resourceConfigs = manager
          .getHelixDataAccessor().getChildValuesMap(key);

      for (String resource : resourceConfigs.keySet())
      {
        HelixProperty resourceConfig = resourceConfigs.get(resource);
        String resourceType = resourceConfig.getRecord().getSimpleField("type");
        if ("DATABASE".equalsIgnoreCase(resourceType))
        {
          databases.add(resource);
          databaseTablesMap.put(resource, new ArrayList<String>());
        }
      }
      for (String dbTable : resourceConfigs.keySet())
      {
        HelixProperty resourceConfig = resourceConfigs.get(dbTable);
        String resourceType = resourceConfig.getRecord().getSimpleField("type");
        if ("TABLE".equalsIgnoreCase(resourceType))
        {
          String dbName = dbTable.split("\\.")[0];
          String table = dbTable.split("\\.")[1];
          databaseTablesMap.get(dbName).add(table);
        }
      }
      int numPartitions = 6;
      Map<String, Connection> connectionMapping = new HashMap<String, Connection>();
      while (true)
      {

        if (_shudPause)
        {
          LOG.info("Pausing load generation");
          lock.lock();
          try
          {
            paused.signal();
            run.await();
            running.signal();
          } finally
          {
            lock.unlock();
          }
          LOG.info("Un-Pausing load generation");
        }
        if (_stop)
        {
          LOG.info("Stopping load generation");
          break;
        }
        String dbName = databases.get(0);
        String tableName = databaseTablesMap.get(dbName).get(0);
        String dbTableName = dbName + "." + tableName;
        Set<InstanceConfig> masters = routingTableProvider.getInstances(
            MySQLConstants.MASTER_SLAVE_RESOURCE_NAME, "MASTER");
        int partition = (int) (Math.random() * 10000 % numPartitions);
        List<InstanceConfig> instances = routingTableProvider.getInstances(
            dbTableName, dbName + "_" + partition + "." + tableName, "ONLINE");
        InstanceConfig master = null;
        for (InstanceConfig instanceConfig : instances)
        {
          if (masters.contains(instanceConfig))
          {
            master = instanceConfig;
            break;
          }
        }
        if (master != null)
        {
          if (!connectionMapping.containsKey(master.getId()))
          {
            String host = master.getHostName();
            String port = master.getRecord().getSimpleField(
                MySQLConstants.MYSQL_PORT);
            String userName = master.getRecord().getSimpleField(
                MySQLConstants.MYSQL_SUPER_USER);
            String password = master.getRecord().getSimpleField(
                MySQLConstants.MYSQL_SUPER_PASSWORD);
            Connection connection = DriverManager.getConnection("jdbc:mysql://"
                + host + ":" + port + "", userName, password);
            connection.setAutoCommit(true);
            connectionMapping.put(master.getId(), connection);
          }
          Connection connection = connectionMapping.get(master.getId());
          String insertQuery = String.format(
              "insert into %s (%s, %s) VALUES(?,?)", tableName, "col1", "col2");

          connection.setCatalog(dbName + "_" + partition);
          PreparedStatement prepareStatement = connection
              .prepareStatement(insertQuery);
          prepareStatement.setInt(1, (int) (Math.random() * 10000));
          prepareStatement.setInt(2, (int) (Math.random() * 10000));
          LOG.info("Running query on master:" + master.getId() + " ," + dbName
              + "_" + partition + " database=" + " query: "
              + prepareStatement.toString());
          prepareStatement.execute();
          Thread.sleep(1000);
        }

      }
    } catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws Exception
  {
    if (args.length == 2)
    {
      String zkAddress = args[0];
      String clusterName = args[1];
      LoadGenerator generator = new LoadGenerator(zkAddress, clusterName);
      new Thread(generator).start();
    } else
    {
      System.err.println("USAGE: java -cp $CLASSPATH LoadGenerator <ZKADDRESS> <CLUSTER_NAME>");
    }
  }
}

package org.apache.fullmatix.mysql;

import org.apache.helix.HelixManager;

public class Context {

  HelixManager _helixManager;
  
  MySQLAdmin _mysqlAdmin;
  
  Replicator _replicator;

  public HelixManager getHelixManager() {
    return _helixManager;
  }

  public void setHelixManager(HelixManager helixManager) {
    _helixManager = helixManager;
  }

  public MySQLAdmin getMysqlAdmin() {
    return _mysqlAdmin;
  }

  public void setMysqlAdmin(MySQLAdmin mysqlAdmin) {
    _mysqlAdmin = mysqlAdmin;
  }

  public Replicator getReplicator() {
    return _replicator;
  }

  public void setReplicator(Replicator replicator) {
    _replicator = replicator;
  }
  
}

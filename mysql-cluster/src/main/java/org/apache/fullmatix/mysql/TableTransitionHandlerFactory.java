package org.apache.fullmatix.mysql;

import org.apache.helix.participant.statemachine.StateModelFactory;

public class TableTransitionHandlerFactory extends StateModelFactory<TableTransitionHandler> {
  private Context _context;

  public TableTransitionHandlerFactory(Context context) {
    _context = context;
  }

  @Override
  public TableTransitionHandler createNewStateModel(String dbPartitionTable) {
    String dbPartition = dbPartitionTable.split("\\.")[0];
    String databaseName = dbPartition.split("_")[0];
    String table = dbPartitionTable.split("\\.")[1];
    return new TableTransitionHandler(_context, databaseName, dbPartition, table);
  }
}

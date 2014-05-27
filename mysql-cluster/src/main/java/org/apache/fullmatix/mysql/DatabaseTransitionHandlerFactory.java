package org.apache.fullmatix.mysql;

import org.apache.helix.participant.statemachine.StateModelFactory;

public class DatabaseTransitionHandlerFactory extends StateModelFactory<DatabaseTransitionHandler> {
  private Context _context;

  public DatabaseTransitionHandlerFactory(Context context) {
    _context = context;
  }

  @Override
  public DatabaseTransitionHandler createNewStateModel(String partition) {
    return new DatabaseTransitionHandler(_context, partition.toString().split("_")[0],
        partition.toString());
  }

}

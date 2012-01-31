package uk.ac.manchester.cs.snee.manager.executer;

import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;

public class Executer extends AutonomicManagerComponent
{

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -5028022484881855975L;

  public Executer(AutonomicManagerImpl autonomicManager)
  {
    manager = autonomicManager;
  }

  public void adapt(Adaptation finalChoice)
  {
    manager.setCurrentQEP(finalChoice.getNewQep());
   // System.exit(0);
    // TODO Auto-generated method stub
    
  }

}

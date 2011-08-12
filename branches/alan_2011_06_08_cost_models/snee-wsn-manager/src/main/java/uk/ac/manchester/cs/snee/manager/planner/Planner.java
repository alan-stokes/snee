package uk.ac.manchester.cs.snee.manager.planner;

import java.util.List;

import uk.ac.manchester.cs.snee.manager.Adapatation;
import uk.ac.manchester.cs.snee.manager.AutonomicManager;

public class Planner 
{

  private AutonomicManager manager;
  
  public Planner(AutonomicManager autonomicManager)
  {
    manager = autonomicManager;
  }

  public Adapatation assessChoices(List<Adapatation> choices)
  {
    return choices.get(0);
    // TODO Auto-generated method stub
  }
  
  

}

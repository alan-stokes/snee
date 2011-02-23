package uk.ac.manchester.cs.snee.compiler.costmodels;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class CostModel
{
  private Agenda agenda;
  private RT routingTree;
  private InstanceDAF instanceDAF;
  
  public CostModel()
  {}

  public float runCardinality() throws OptimizationException 
  {
    long beta = agenda.getBufferingFactor();
	  InstanceOperator rootOperator = instanceDAF.getRoot();
	  Site rootSite = routingTree.getRoot();
	  return rootOperator.getInstanceCardinality(rootSite, instanceDAF, beta);
  }
  
  public void addAgenda(Agenda agenda)
  {
    this.agenda = agenda;
  }
  
  public void addRoutingTree(RT routingTree)
  {
    this.routingTree = routingTree;
  }
  
  public void addInstanceDAF(InstanceDAF daf)
  {
    this.instanceDAF = daf;
  }
}
package uk.ac.manchester.cs.snee.compiler.costmodels;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;

public class CostModel
{
  Agenda agenda;
  RT routingTree;
  DAF daf;
  float cardinality;
  
  public CostModel(SensorNetworkQueryPlan qep) throws OptimizationException 
  {
	agenda = qep.getAgenda();
	routingTree = qep.getRT();
	daf = qep.getDAF();
	
	cardinality = runCardinality();
	System.out.println("The cardinality of the query is " + cardinality);
  }

  private float runCardinality() throws OptimizationException 
  {
    long beta = agenda.getBufferingFactor();
	SensornetOperator rootOperator = daf.getRootOperator();
	Site rootSite = routingTree.getRoot();
	return rootOperator.getInstanceCardinality(rootSite, daf, beta);
  }
  
  public float getCardianlity()
  {
    return cardinality;
  }
}
package uk.ac.manchester.cs.snee.compiler.costmodels;

import java.util.ArrayList;
import java.util.HashMap;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel.CardinalityDataStructure;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAcquireOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrEvalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrInitOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrMergeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetNestedLoopJoinOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetProjectOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetRStreamOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetSelectOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetSingleStepAggregationOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetWindowOperator;

public abstract class CostModel
{
  protected AgendaIOT agenda;
  protected RT routingTree;
  
  protected CostModelDataStructure model(InstanceOperator operator) 
  throws OptimizationException
  {
    //System.out.println("within operator " + operator.getID());
    if(operator.getSensornetOperator() instanceof SensornetAcquireOperator)
    {
      return acquireCard(operator);
    }
    else if(operator.getSensornetOperator() instanceof SensornetSingleStepAggregationOperator)
    {
      return aggerateCard(operator);
    }
    else if(operator.getSensornetOperator() instanceof SensornetAggrEvalOperator)
    {
      return aggerateCard(operator);
    }
    else if(operator.getSensornetOperator() instanceof SensornetAggrInitOperator)
    {
      return aggerateCard(operator);
    }
    else if(operator.getSensornetOperator() instanceof SensornetAggrMergeOperator)
    {
      return aggerateCard(operator);
    }
    else if(operator.getSensornetOperator() instanceof SensornetDeliverOperator)
    {
      return deliverModel(operator);
    }
    else if(operator.getSensornetOperator() instanceof SensornetNestedLoopJoinOperator)
    {
      return joinCard(operator);
    }
    else if(operator.getSensornetOperator() instanceof SensornetProjectOperator)
    {
      InstanceOperator op = (InstanceOperator)(operator.getInstanceInput(0));
      return model(op);
    }
    else if(operator.getSensornetOperator() instanceof SensornetRStreamOperator)
    {
      return RStreamCard(operator);
    }
    else if(operator.getSensornetOperator() instanceof SensornetSelectOperator)
    {
      return selectCard(operator);
    }
    else if(operator.getSensornetOperator() instanceof SensornetWindowOperator)
    {
      return windowCard(operator);
    }
    else if(operator instanceof InstanceExchangePart)
    {
      return exchangeCard(operator);
    }
    else
    {
      String msg = "Unsupported operator " + operator.getSensornetOperator().getOperatorName();
      System.out.println("UNKNOWN OPORATEOR " + msg);
      return new CardinalityDataStructure(0);
    }
  }

  protected abstract CostModelDataStructure deliverModel(InstanceOperator operator)
  throws OptimizationException;
  protected abstract CostModelDataStructure exchangeCard(InstanceOperator operator)
  throws OptimizationException;
  protected abstract CostModelDataStructure windowCard(InstanceOperator operator)
  throws OptimizationException;
  protected abstract CostModelDataStructure selectCard(InstanceOperator operator)
  throws OptimizationException;
  protected abstract CostModelDataStructure RStreamCard(InstanceOperator operator)
  throws OptimizationException;
  protected abstract CostModelDataStructure joinCard(InstanceOperator operator)
  throws OptimizationException;
  protected abstract CostModelDataStructure aggerateCard(InstanceOperator operator)
  throws OptimizationException;
  protected abstract CostModelDataStructure acquireCard(InstanceOperator operator)
  throws OptimizationException;
  
  public CostModelDataStructure model(InstanceOperator operator, ArrayList<Integer> inputs,
		                              HashMap<String, Integer> tuples, Long beta) 
  throws OptimizationException
  {
    //System.out.println("within operator " + operator.getID());
    if(operator.getSensornetOperator() instanceof SensornetAcquireOperator)
    {
      return acquireCard(operator, inputs);
    }
    else if(operator.getSensornetOperator() instanceof SensornetSingleStepAggregationOperator)
    {
      return aggerateCard(operator, inputs);
    }
    else if(operator.getSensornetOperator() instanceof SensornetAggrEvalOperator)
    {
      return aggerateCard(operator, inputs);
    }
    else if(operator.getSensornetOperator() instanceof SensornetAggrInitOperator)
    {
      return aggerateCard(operator, inputs);
    }
    else if(operator.getSensornetOperator() instanceof SensornetAggrMergeOperator)
    {
      return aggerateCard(operator, inputs);
    }
    else if(operator.getSensornetOperator() instanceof SensornetDeliverOperator)
    {
      return deliverModel(operator, inputs);
    }
    else if(operator.getSensornetOperator() instanceof SensornetNestedLoopJoinOperator)
    {
      return joinCard(operator, inputs, tuples, beta);
    }
    else if(operator.getSensornetOperator() instanceof SensornetProjectOperator)
    {
      InstanceOperator op = (InstanceOperator)(operator.getInstanceInput(0));
      return model(op, inputs, tuples, beta);
    }
    else if(operator.getSensornetOperator() instanceof SensornetRStreamOperator)
    {
      return RStreamCard(operator, inputs);
    }
    else if(operator.getSensornetOperator() instanceof SensornetSelectOperator)
    {
      return selectCard(operator, inputs);
    }
    else if(operator.getSensornetOperator() instanceof SensornetWindowOperator)
    {
      return windowCard(operator, inputs);
    }
    else if(operator instanceof InstanceExchangePart)
    {
      return exchangeCard(operator, inputs);
    }
    else
    {
      String msg = "Unsupported operator " + operator.getSensornetOperator().getOperatorName();
      System.out.println("UNKNOWN OPORATEOR " + msg);
      return new CardinalityDataStructure(0);
    }
  }
  
  protected abstract CostModelDataStructure deliverModel(InstanceOperator operator, ArrayList<Integer> inputs)
  throws OptimizationException;
  protected abstract CostModelDataStructure exchangeCard(InstanceOperator operator, ArrayList<Integer> inputs)
  throws OptimizationException;
  protected abstract CostModelDataStructure windowCard(InstanceOperator operator, ArrayList<Integer> inputs)
  throws OptimizationException;
  protected abstract CostModelDataStructure selectCard(InstanceOperator operator, ArrayList<Integer> inputs)
  throws OptimizationException;
  protected abstract CostModelDataStructure RStreamCard(InstanceOperator operator, ArrayList<Integer> inputs)
  throws OptimizationException;
  protected abstract CostModelDataStructure joinCard(InstanceOperator operator, ArrayList<Integer> inputs, 
                                                     HashMap<String, Integer> tuples, long beta)
  throws OptimizationException;
  protected abstract CostModelDataStructure aggerateCard(InstanceOperator operator, ArrayList<Integer> inputs)
  throws OptimizationException;
  protected abstract CostModelDataStructure acquireCard(InstanceOperator operator, ArrayList<Integer> inputs)
  throws OptimizationException;
  
  public void setSiteDead(String deadNode)
  {
    Site toDie = routingTree.getSite(deadNode);
    toDie.setisDead(true);  
  }

  public long getBeta()
  {
    return agenda.getBufferingFactor();
  }
}

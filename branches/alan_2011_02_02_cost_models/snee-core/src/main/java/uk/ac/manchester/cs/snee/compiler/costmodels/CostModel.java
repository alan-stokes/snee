package uk.ac.manchester.cs.snee.compiler.costmodels;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.WindowOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAcquireOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrEvalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrInitOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrMergeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetNestedLoopJoinOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetProjectOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetRStreamOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetSelectOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetWindowOperator;

public class CostModel
{
  private Agenda agenda;
  private RT routingTree;
  private InstanceDAF instanceDAF;
  private boolean rowComplete = false;
  private int rowWindowBuffer = 0;
  private ArrayList<Integer> rowWindowBufferPreivousValues = new ArrayList<Integer>();
  private boolean timeComplete = false;
  private int iteration = 1;
  
  public CostModel()
  {}

  public void runCardinality() throws OptimizationException 
  {
	  InstanceOperator rootOperator = instanceDAF.getRoot();
	  Site rootSite = routingTree.getRoot();
	  while(!timeComplete || !rowComplete)
	  {
	    rowComplete = true;
	    timeComplete = true;
	    CardinalityStruct value = cardinalities(rootOperator, rootSite);
	    float card = value.getNoWindows() * value.getCardPerWindow();
      System.out.println("the cardinality of this query for agenda cycle " + iteration + " is " + card);
      iteration ++;
	  }
  }
  
  private CardinalityStruct cardinalities(InstanceOperator operator, Site node) 
  throws OptimizationException
  {
    System.out.println("within operator " + operator.getID());
    if(operator.isNodeDead())
      return new CardinalityStruct(0,0);
    else
    {
      if(operator.getInstanceOperator() instanceof SensornetAcquireOperator)
      {
        CardinalityStruct output = new CardinalityStruct(1, (1 * operator.selectivity()));
        System.out.println(operator.getID() + " = " + output);
        return output;
      }
      else if(operator.getInstanceOperator() instanceof SensornetAggrEvalOperator)
      {
        return aggerateCard(operator, node);
      }
      else if(operator.getInstanceOperator() instanceof SensornetAggrInitOperator)
      {
        return aggerateCard(operator, node);
      }
      else if(operator.getInstanceOperator() instanceof SensornetAggrMergeOperator)
      {
        return aggerateCard(operator, node);
      }
      else if(operator.getInstanceOperator() instanceof SensornetDeliverOperator)
      {
        InstanceOperator op = (InstanceOperator)(operator.getInput(0));
        return cardinalities(op, node);
      }
      else if(operator.getInstanceOperator() instanceof SensornetNestedLoopJoinOperator)
      {
        return joinCard(operator, node);
      }
      else if(operator.getInstanceOperator() instanceof SensornetProjectOperator)
      {
        InstanceOperator op = (InstanceOperator)(operator.getInput(0));
        return cardinalities(op, node);
      }
      else if(operator.getInstanceOperator() instanceof SensornetRStreamOperator)
      {
        InstanceOperator op = (InstanceOperator)(operator.getInput(0));
        CardinalityStruct outputCard = cardinalities(op, node);
        System.out.println(operator.getID() + " = " + outputCard);
        return outputCard;
      }
      else if(operator.getInstanceOperator() instanceof SensornetSelectOperator)
      {
        InstanceOperator op = (InstanceOperator)(operator.getInput(0));
        CardinalityStruct inputCard = cardinalities(op, node);
        float outputCardinality = (inputCard.getCardPerWindow() * operator.selectivity());
        CardinalityStruct outputCard = new CardinalityStruct(inputCard.getNoWindows(), outputCardinality);
        System.out.println(operator.getID() + " = " + outputCard);
        return outputCard;
      }
      else if(operator.getInstanceOperator() instanceof SensornetWindowOperator)
      {
        return windowCard(operator, node);
      }
      else if(operator instanceof InstanceExchangePart)
      {
        return exchangeCard(operator, node);
      }
      else
      {
        String msg = "Unsupported operator " + operator.getInstanceOperator().getOperatorName();
        System.out.println("UNKNOWN OPORATEOR " + msg);
        return new CardinalityStruct(0,0);
      }
    }
  }
  
  private CardinalityStruct exchangeCard(InstanceOperator operator, Site node)
  throws OptimizationException
  {
    if(((InstanceExchangePart) operator).getPrevious() != null)//path
    {
      CardinalityStruct outputCard =cardinalities(((InstanceExchangePart) operator).getPrevious(), node);
      System.out.println(operator.getID() + " = " + outputCard);
      return outputCard;
    }
    else//hit new frag
    {
      if(((InstanceExchangePart) operator).getSourceFrag().isLeaf())
      {
        CardinalityStruct inputCard = cardinalities(((InstanceExchangePart) operator).getSourceFrag().getRootOperator(), node);
        inputCard.setNoWindows(inputCard.getNoWindows() * agenda.getBufferingFactor());
        System.out.println(operator.getID() + " = " + inputCard);
        return inputCard;
      }
      else
      {
        CardinalityStruct outputCard = cardinalities(((InstanceExchangePart) operator).getSourceFrag().getRootOperator(), node);
        System.out.println(operator.getID() + " = " + outputCard);
        return outputCard;
      }
    }
  }

  public CardinalityStruct windowCard(InstanceOperator operator, Site node)
  throws OptimizationException
  {
    WindowOperator logicalOp = (WindowOperator) operator.getInstanceOperator().getLogicalOperator();
    if(logicalOp.getTimeScope())//time window
    {
      InstanceOperator op = (InstanceOperator)(operator.getInput(0));
      CardinalityStruct inputCard = cardinalities(op, node);
      CardinalityStruct output = new  CardinalityStruct(1, inputCard.getCardPerWindow());
      System.out.println(op.getID() + " = " + output);
      return output;
      //agenda.
    }
    else //row window
    {
      InstanceOperator op = (InstanceOperator)(operator.getInput(0));
      CardinalityStruct inputCard = cardinalities(op, node);
      //get row size
      int windowSizeInRows = logicalOp.getRowSlide();
      float numberOfWindows = (inputCard.getCardPerWindow() + rowWindowBuffer) / windowSizeInRows;
      rowWindowBuffer = (int) ((inputCard.getCardPerWindow() + rowWindowBuffer) % windowSizeInRows);
      if(rowWindowBufferPreivousValues.contains(rowWindowBuffer))
        rowComplete = true;
      else
        rowComplete = false;
      rowWindowBufferPreivousValues.add(rowWindowBuffer);
      CardinalityStruct output = new CardinalityStruct(numberOfWindows, windowSizeInRows);  
      System.out.println(op.getID() + " = " + output);
      return output;
    }
  }
  
  public CardinalityStruct aggerateCard(InstanceOperator operator, Site node)
  throws OptimizationException
  {
    ArrayList<Float> inputs = new ArrayList<Float>();
    for(int index = 0; index < operator.getInDegree(); index++)
    {
      CardinalityStruct card;
      InstanceOperator op = (InstanceOperator)(operator.getInput(index));
      card = cardinalities(op, node);
      inputs.add(card.getNoWindows());
    }
    Collections.sort(inputs);
    CardinalityStruct output = new CardinalityStruct(inputs.get(0), 1);
    System.out.println(operator.getID() + " = " + output);
    return output;
  }
  
  public CardinalityStruct joinCard(InstanceOperator operator, Site node)
  throws OptimizationException
  {
    ArrayList<Float> inputWindows = new ArrayList<Float>();
    ArrayList<Float> inputCards = new ArrayList<Float>();
    for(int index = 0; index < operator.getInDegree(); index++)
    {
      CardinalityStruct card;
      InstanceOperator op = (InstanceOperator)(operator.getInput(index));
      card = cardinalities(op, node);
      inputWindows.add(card.getNoWindows());
      inputCards.add(card.getCardPerWindow());
    }
    Collections.sort(inputWindows);
    float card = inputCards.get(0) * inputCards.get(1) * operator.selectivity();
    CardinalityStruct output =new CardinalityStruct(inputWindows.get(0), card);
    System.out.println(operator.getID() + " = " + output);
    return output;
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
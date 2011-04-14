package uk.ac.manchester.cs.snee.compiler.costmodels;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.WindowOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAcquireOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrEvalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrInitOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrMergeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetNestedLoopJoinOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetProjectOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetRStreamOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetSelectOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetWindowOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;

public class CardinalityEstimatedCostModel
{
  private IOT instanceDAF;
  private Agenda agenda;
  private RT routingTree;
  private float epochResult;
  
  public CardinalityEstimatedCostModel(QueryExecutionPlan qep)
  {
	  SensorNetworkQueryPlan sqep= (SensorNetworkQueryPlan) qep;
	  agenda = sqep.getAgenda();
	  routingTree = sqep.getRT();
	  instanceDAF = sqep.getInstanceDAF(); 
  }
  
  public float returnEpochResult() throws OptimizationException 
  {
	  return epochResult;
  }
  
  public float returnAgendaExecutionResult() throws OptimizationException 
  {
	  float epochResult = returnEpochResult();
	  return epochResult * agenda.getBufferingFactor();
  }
  
  
  public void runModel() throws OptimizationException 
  {
	  InstanceOperator rootOperator = instanceDAF.getRoot();
	  CardinalityStruct result = cardinalities(rootOperator);
	  epochResult = result.getCard();
  }
  
  private CardinalityStruct cardinalities(InstanceOperator operator) 
  throws OptimizationException
  {
   // System.out.println("within operator " + operator.getID());
    if(operator.isNodeDead())
      return new CardinalityStruct(0);
    else
    {
      if(operator.getInstanceOperator() instanceof SensornetAcquireOperator)
      {
        return acquireCard(operator);
      }
      else if(operator.getInstanceOperator() instanceof SensornetAggrEvalOperator)
      {
        return aggerateCard(operator);
      }
      else if(operator.getInstanceOperator() instanceof SensornetAggrInitOperator)
      {
        return aggerateCard(operator);
      }
      else if(operator.getInstanceOperator() instanceof SensornetAggrMergeOperator)
      {
        return aggerateCard(operator);
      }
      else if(operator.getInstanceOperator() instanceof SensornetDeliverOperator)
      {
    	ArrayList<CardinalityStruct> reducedInputs = reduceInputs(operator);
        CardinalityStruct input = reducedInputs.get(0);
        return input;
      }
      else if(operator.getInstanceOperator() instanceof SensornetNestedLoopJoinOperator)
      {
        return joinCard(operator);
      }
      else if(operator.getInstanceOperator() instanceof SensornetProjectOperator)
      {
        InstanceOperator op = (InstanceOperator)(operator.getInstanceInput(0));
        return cardinalities(op);
      }
      else if(operator.getInstanceOperator() instanceof SensornetRStreamOperator)
      {
        return RStreamCard(operator);
      }
      else if(operator.getInstanceOperator() instanceof SensornetSelectOperator)
      {
        return selectCard(operator);
      }
      else if(operator.getInstanceOperator() instanceof SensornetWindowOperator)
      {
        return windowCard(operator);
      }
      else if(operator instanceof InstanceExchangePart)
      {
        return exchangeCard(operator);
      }
      else
      {
        String msg = "Unsupported operator " + operator.getInstanceOperator().getOperatorName();
        System.out.println("UNKNOWN OPORATEOR " + msg);
        return new CardinalityStruct(0);
      }
    }
  }
  
  private CardinalityStruct selectCard(InstanceOperator inputOperator) 
  throws OptimizationException
  {
	ArrayList<CardinalityStruct> reducedInputs = reduceInputs(inputOperator);
    CardinalityStruct input = reducedInputs.get(0);
    CardinalityStruct output;
    if(input.isStream())
    {
      output = new CardinalityStruct(input.getCardOfStream() * inputOperator.selectivity());
      //System.out.println(inputOperator.getID() + " inputCard= " + input);
    //  System.out.println(inputOperator.getID() + " outputCard= " + output);
    }
    else
    {
      float windowStreamCard = input.getCardOfStream();
      float windowCard = input.getWindowCard() * inputOperator.selectivity();
      output = new CardinalityStruct(windowStreamCard, windowCard);
      //System.out.println(inputOperator.getID() + " inputCard= " + input.getCard());
      //System.out.println(inputOperator.getID() + " outputCard= " + output.getCard());  
    }
    return output;
  }

  private CardinalityStruct RStreamCard(InstanceOperator inputOperator) 
  throws OptimizationException
  {
	ArrayList<CardinalityStruct> reducedInputs = reduceInputs(inputOperator);
    CardinalityStruct input = reducedInputs.get(0);
    CardinalityStruct output;
    output = new CardinalityStruct(input.getCardOfStream() * input.getWindowCard());
    
    //System.out.println(inputOperator.getID() + " inputCard= " + input);
    //System.out.println(inputOperator.getID() + " outputCard= " + output);
    return output; 
  }

  private CardinalityStruct acquireCard(InstanceOperator inputOperator)
  {
    float output = 1 * inputOperator.selectivity();
    CardinalityStruct out = new CardinalityStruct(output);
   // System.out.println(inputOperator.getID() + " outputCard= " + output);
    List<Attribute> attributes = inputOperator.getInstanceOperator().getLogicalOperator().getAttributes();
    out.setExtentName(attributes.get(1).toString());
    return out;
  }

  private CardinalityStruct exchangeCard(InstanceOperator inputOperator)
  throws OptimizationException
  {
    CardinalityStruct input;
    if(((InstanceExchangePart) inputOperator).getPrevious() != null)//path
    {
      input =cardinalities(((InstanceExchangePart) inputOperator).getPrevious());
    }
    else//hit new frag
    {
      InstanceExchangePart producer = ((InstanceExchangePart)inputOperator);
      input = cardinalities( (InstanceOperator) producer.getInstanceInput(0));
    } 
    
   // System.out.println(inputOperator.getID() + " inputCard= " + input);
   // System.out.println(inputOperator.getID() + " outputCard= " + input);
    return input;
  }

  public CardinalityStruct windowCard(InstanceOperator inputOperator)
  throws OptimizationException
  {
    WindowOperator logicalOp = (WindowOperator) inputOperator.getInstanceOperator().getLogicalOperator();
    float to = logicalOp.getTo();
    float from = logicalOp.getFrom();
    float length = (to-from)+1;
    float slide;
    
    if(logicalOp.getTimeScope())
      slide = logicalOp.getTimeSlide();
    else
      slide = logicalOp.getRowSlide();
       
    InstanceOperator childOperator = (InstanceOperator)(inputOperator.getInstanceInput(0));
    CardinalityStruct input = cardinalities(childOperator);
      
    float noWindows;
    if(slide == 0)//now window, to stop infinity
      noWindows = 1;
    else
      noWindows = length / slide;
    
    float winCard = input.getCard();
    CardinalityStruct output = new CardinalityStruct(noWindows, winCard);
    output.setExtentName(input.getExtentName());

  //  System.out.println(inputOperator.getID() + " inputCard= " + input);
   // System.out.println(inputOperator.getID() + " outputCard= " + output);
    return output;
  }
  
  public CardinalityStruct aggerateCard(InstanceOperator inputOperator)
  throws OptimizationException
  {
    CardinalityStruct output = null;
    ArrayList<CardinalityStruct> reducedInputs = reduceInputs(inputOperator);
  //  System.out.println("aggerate newinputs size is " + reducedInputs.size());
    
    if(reducedInputs.size() == 1)  //init
    {
        output = new CardinalityStruct(reducedInputs.size(), 1);
    }
    else
    {
    	output = new CardinalityStruct(reducedInputs.size(), 1);
    }
    output.setExtentName(reducedInputs.get(0).getExtentName());
  //  System.out.println(inputOperator.getID() + " inputCard= " + reducedInputs.size());
   // System.out.println(inputOperator.getID() + " outputCard= " + output);
    return output;
  }
  
  public CardinalityStruct joinCard(InstanceOperator inputOperator)
  throws OptimizationException
  {
	 
    ArrayList<CardinalityStruct> reducedInputs = reduceInputs(inputOperator);
   // System.out.println("join newInput size is " + reducedInputs.size());
    CardinalityStruct inputR = reducedInputs.get(0);
    CardinalityStruct inputL = reducedInputs.get(1);
	
    float windowStreamCard;
    float windowCard;
    
    if(inputL.isStream())
    {
      windowStreamCard = 1;
      windowCard = inputL.getCardOfStream() * inputR.getCardOfStream() * inputOperator.selectivity();
   //   System.out.println(inputOperator.getID() + " inputCardL= " + 1 + " Stream with Card "+ inputL.getCardOfStream());
   //   System.out.println(inputOperator.getID() + " inputCardR= " + 1 + " Stream with Card "+ inputL.getCardOfStream());
    }
    else
    {
    	
      windowStreamCard = inputL.getCardOfStream();
      windowCard = inputL.getWindowCard() * inputR.getWindowCard() * inputOperator.selectivity();
     // System.out.println(inputOperator.getID() + " inputCardL= " + inputL.getCardOfStream() + " inputs each with "+ inputL.getWindowCard());
     // System.out.println(inputOperator.getID() + " inputCardR= " + inputR.getCardOfStream() + " inputs each with "+ inputR.getWindowCard());
    }
    CardinalityStruct output = new CardinalityStruct(windowStreamCard, windowCard);
    System.out.println(inputOperator.getID() + " outputCard= " + output);
    return output;
  }
 
  public ArrayList<CardinalityStruct> reduceInputs(InstanceOperator inputOperator) throws OptimizationException
  {
	ArrayList<CardinalityStruct> inputs = new ArrayList<CardinalityStruct>();
		
	for(int x = 0; x < inputOperator.getInDegree(); x ++)
	{
      inputs.add(cardinalities((InstanceOperator) inputOperator.getInstanceInput(x)));
	}
	  
    ArrayList<CardinalityStruct> outputs = new ArrayList<CardinalityStruct>();
    outputs.add(inputs.get(0));
    
    for(int inputsIndex = 1; inputsIndex < inputs.size(); inputsIndex++)
    {
      CardinalityStruct input = inputs.get(inputsIndex);
      int testIndex = 0;
      boolean stored = false;
      while(testIndex < outputs.size() && !stored)
      {
        CardinalityStruct test = outputs.get(testIndex);
        if(test.getExtentName().equals(input.getExtentName()))
        {
          test.setCardOfStream(test.getCardOfStream() + input.getDirectCard());
          stored = true;
        }
        else
        {
          testIndex++;
        }
      }
      if(!stored)
        outputs.add(input);
    }
    return outputs;
  }
  
  public void setSiteDead(int siteID)
  {
	  Site toDie = routingTree.getSite(siteID);
	  toDie.setisDead(true);  
  }
}
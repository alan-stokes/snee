package uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel;

import java.util.ArrayList;
import java.util.List;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.operators.logical.WindowOperator;
import uk.ac.manchester.cs.snee.compiler.costmodels.CostModel;
import uk.ac.manchester.cs.snee.compiler.costmodels.CostModelDataStructure;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;

public class CardinalityEstimatedCostModel extends CostModel
{
  private IOT instanceDAF;
  private float epochResult;
  
  public CardinalityEstimatedCostModel(QueryExecutionPlan qep)
  {
	  SensorNetworkQueryPlan sqep= (SensorNetworkQueryPlan) qep;
	  agenda = sqep.getAgendaIOT();
	  routingTree = sqep.getRT();
	  instanceDAF = sqep.getIOT(); 
  }
  
  public CardinalityEstimatedCostModel(SensorNetworkQueryPlan sqep)
  {
    agenda = sqep.getAgendaIOT();
    routingTree = sqep.getRT();
    instanceDAF = sqep.getIOT(); 
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
	  if(instanceDAF != null)
    {
	    InstanceOperator rootOperator = instanceDAF.getRoot();
	    CardinalityDataStructure result = (CardinalityDataStructure) model(rootOperator);
	    epochResult = result.getCard();
	  }
  }
  
  protected CardinalityDataStructure selectCard(InstanceOperator inputOperator) 
  throws OptimizationException
  {
    if(inputOperator.isNodeDead())
      return new CardinalityDataStructure(0);
    
	  ArrayList<CardinalityDataStructure> reducedInputs = reduceInputs(inputOperator);
    CardinalityDataStructure input = reducedInputs.get(0);
    CardinalityDataStructure output;
    if(input.isStream())
    {
      output = new CardinalityDataStructure(input.getCardOfStream() * inputOperator.selectivity());
      //System.out.println(inputOperator.getID() + " inputCard= " + input);
      //System.out.println(inputOperator.getID() + " outputCard= " + output);
    }
    else
    {
      float windowStreamCard = input.getCardOfStream();
      float windowCard = input.getWindowCard() * inputOperator.selectivity();
      output = new CardinalityDataStructure(windowStreamCard, windowCard);
      //System.out.println(inputOperator.getID() + " inputCard= " + input.getCard());
      //System.out.println(inputOperator.getID() + " outputCard= " + output.getCard());  
    }
    return output;
  }

  protected CardinalityDataStructure RStreamCard(InstanceOperator inputOperator) 
  throws OptimizationException
  {
    if(inputOperator.isNodeDead())
      return new CardinalityDataStructure(0);
    
	  ArrayList<CardinalityDataStructure> reducedInputs = reduceInputs(inputOperator);
    CardinalityDataStructure input = reducedInputs.get(0);
    CardinalityDataStructure output;
    output = new CardinalityDataStructure(input.getCardOfStream() * input.getWindowCard());
    output.setStream(true);
    //System.out.println(inputOperator.getID() + " inputCard= " + input);
    //System.out.println(inputOperator.getID() + " outputCard= " + output);
    return output; 
  }

  protected CardinalityDataStructure acquireCard(InstanceOperator inputOperator)
  {
    if(inputOperator.isNodeDead())
      return new CardinalityDataStructure(0);
    
    float output = 1 * inputOperator.selectivity();
    CardinalityDataStructure out = new CardinalityDataStructure(output);
    //System.out.println(inputOperator.getID() + " outputCard= " + output);
    List<Attribute> attributes = inputOperator.getSensornetOperator().getLogicalOperator().getAttributes();
    out.setExtentName(attributes.get(1).toString());
    return out;
  }

  protected CardinalityDataStructure exchangeCard(InstanceOperator inputOperator)
  throws OptimizationException
  {
    if(inputOperator.isNodeDead())
      return new CardinalityDataStructure(0);
    
    CardinalityDataStructure input;
    if(((InstanceExchangePart) inputOperator).getPrevious() != null)//path
    {
      input = (CardinalityDataStructure) model(((InstanceExchangePart) inputOperator).getPrevious());
    }
    else//hit new frag
    {
      InstanceExchangePart producer = ((InstanceExchangePart)inputOperator);
      input = (CardinalityDataStructure) model( (InstanceOperator) producer.getInstanceInput(0));
    } 
    
    //System.out.println(inputOperator.getID() + " inputCard= " + input);
    //System.out.println(inputOperator.getID() + " outputCard= " + input);
    return input;
  }

  protected CardinalityDataStructure windowCard(InstanceOperator inputOperator)
  throws OptimizationException
  {
    if(inputOperator.isNodeDead())
      return new CardinalityDataStructure(0);
    
    WindowOperator logicalOp = (WindowOperator) inputOperator.getSensornetOperator().getLogicalOperator();
    float to = logicalOp.getTo();
    float from = logicalOp.getFrom();
    float length = (to-from)+1;
    float slide;
    
    if(logicalOp.getTimeScope())
      slide = logicalOp.getTimeSlide();
    else
      slide = logicalOp.getRowSlide();
       
    InstanceOperator childOperator = (InstanceOperator)(inputOperator.getInstanceInput(0));
    CardinalityDataStructure input = (CardinalityDataStructure) model(childOperator);
      
    float noWindows;
    if(slide == 0)//now window, to stop infinity
      noWindows = 1;
    else
      noWindows = length / slide;
    
    float winCard = input.getCard();
    CardinalityDataStructure output = new CardinalityDataStructure(noWindows, winCard);
    output.setExtentName(input.getExtentName());

   //System.out.println(inputOperator.getID() + " inputCard= " + input);
   //System.out.println(inputOperator.getID() + " outputCard= " + output);
    return output;
  }
  
  protected CardinalityDataStructure aggerateCard(InstanceOperator inputOperator)
  throws OptimizationException
  {
    if(inputOperator.isNodeDead())
      return new CardinalityDataStructure(0);
    
    CardinalityDataStructure output = null;
    ArrayList<CardinalityDataStructure> reducedInputs = reduceInputs(inputOperator);
    //System.out.println("aggerate newinputs size is " + reducedInputs.size());
    
    if(reducedInputs.size() == 1)  //init
    {
        output = new CardinalityDataStructure(reducedInputs.size(), 1);
    }
    else
    {
    	output = new CardinalityDataStructure(reducedInputs.size(), 1);
    }
    output.setExtentName(reducedInputs.get(0).getExtentName());
    //System.out.println(inputOperator.getID() + " inputCard= " + reducedInputs.size());
    //System.out.println(inputOperator.getID() + " outputCard= " + output);
    return output;
  }
  
  protected CardinalityDataStructure joinCard(InstanceOperator inputOperator)
  throws OptimizationException
  {
    if(inputOperator.isNodeDead())
      return new CardinalityDataStructure(0);
    
    ArrayList<CardinalityDataStructure> reducedInputs = reduceInputs(inputOperator);
    //System.out.println("join newInput size is " + reducedInputs.size());
    CardinalityDataStructure inputR = reducedInputs.get(0);
    CardinalityDataStructure inputL = reducedInputs.get(1);
	
    float windowStreamCard;
    float windowCard;
    
    if(inputL.isStream())
    {
      windowStreamCard = 1;
      windowCard = inputL.getCardOfStream() * inputR.getCardOfStream() * inputOperator.selectivity();
     //System.out.println(inputOperator.getID() + " inputCardL= " + 1 + " Stream with Card "+ inputL.getCardOfStream());
     //System.out.println(inputOperator.getID() + " inputCardR= " + 1 + " Stream with Card "+ inputL.getCardOfStream());
    }
    else
    {
    	
      windowStreamCard = inputL.getCardOfStream();
      windowCard = inputL.getWindowCard() * inputR.getWindowCard() * inputOperator.selectivity();
      //System.out.println(inputOperator.getID() + " inputCardL= " + inputL.getCardOfStream() + " inputs each with "+ inputL.getWindowCard());
      //System.out.println(inputOperator.getID() + " inputCardR= " + inputR.getCardOfStream() + " inputs each with "+ inputR.getWindowCard());
    }
    CardinalityDataStructure output = new CardinalityDataStructure(windowStreamCard, windowCard);
    //System.out.println(inputOperator.getID() + " outputCard= " + output);
    return output;
  }
  
  protected CostModelDataStructure deliverModel(InstanceOperator inputOperator)
  throws OptimizationException
  {
    if(inputOperator.isNodeDead())
      return new CardinalityDataStructure(0);
    
    ArrayList<CardinalityDataStructure> reducedInputs = reduceInputs(inputOperator);
    CostModelDataStructure input = reducedInputs.get(0);
    return input;
  }
 
  protected ArrayList<CardinalityDataStructure> reduceInputs(InstanceOperator inputOperator) throws OptimizationException
  {
    ArrayList<CostModelDataStructure> inputs = new ArrayList<CostModelDataStructure>();
    
    for(int x = 0; x < inputOperator.getInDegree(); x ++)
    {
      CardinalityDataStructure input = 
        (CardinalityDataStructure) model((InstanceOperator) inputOperator.getInstanceInput(x));
      if(input.getCard() != 0)
        inputs.add(input);
    }
    
    ArrayList<CardinalityDataStructure> outputs = new ArrayList<CardinalityDataStructure>();
    if(inputs.size() != 0)
      outputs.add((CardinalityDataStructure) inputs.get(0));
    else
      outputs.add(new CardinalityDataStructure(0));
    
    for(int inputsIndex = 1; inputsIndex < inputs.size(); inputsIndex++)
    {
      CardinalityDataStructure input = (CardinalityDataStructure) inputs.get(inputsIndex);
      int testIndex = 0;
      boolean stored = false;
      while(testIndex < outputs.size() && !stored)
      {
        CardinalityDataStructure test = outputs.get(testIndex);
        
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
}
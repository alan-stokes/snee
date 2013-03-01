package uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
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
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetSingleStepAggregationOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetWindowOperator;
import uk.ac.manchester.cs.snee.compiler.costmodels.CostModel;
import uk.ac.manchester.cs.snee.compiler.costmodels.CostModelDataStructure;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
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
  
  public CardinalityEstimatedCostModel(AgendaIOT agenda, RT routingTree,  IOT iot)
  {
    this.agenda = agenda;
    this.routingTree = routingTree;
    instanceDAF = iot; 
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
    
    if(reducedInputs.size() < 2)
    {
      CardinalityDataStructure output = 
        new CardinalityDataStructure(0, 0);
      return output;
    }
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
      if(inputR.isStream())
      {
    	  windowStreamCard = 1;
    	  windowCard = inputL.getWindowCard() * inputR.getCardOfStream() * inputOperator.selectivity();
      }
      else
      {
        windowStreamCard = inputL.getCardOfStream();
        windowCard = inputL.getWindowCard() * inputR.getWindowCard() * inputOperator.selectivity();
      }
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

  
  protected CardinalityDataStructureChannel deliverModel(InstanceOperator operator,
                                                         CollectionOfPackets inputs) 
  throws OptimizationException
  {
    if(operator.isNodeDead())
      return new CardinalityDataStructureChannel(new ArrayList<Window>());
    String extent = operator.getExtent();
    ArrayList<Window> outputWindows = new ArrayList<Window>();
    Iterator<Window> inputWindows =inputs.getWindowsOfExtent(extent).iterator();
    while(inputWindows.hasNext())
    {
      Window input = inputWindows.next();
      outputWindows.add(new Window(input.getTuples(), input.getWindowID()));   
    }
    
    return new CardinalityDataStructureChannel(outputWindows);
  }

  
  protected CardinalityDataStructureChannel exchangeCard(InstanceOperator operator,
                                                         CollectionOfPackets inputs)
  throws OptimizationException
  {
    if(operator.isNodeDead())
      return new CardinalityDataStructureChannel(new ArrayList<Window>());
    String extent = operator.getExtent();
    return new CardinalityDataStructureChannel(inputs.getWindowsOfExtent(extent));
  }

  
  protected CardinalityDataStructureChannel windowCard(InstanceOperator operator,
                                                       CollectionOfPackets inputs)
  throws OptimizationException
  {
    if(operator.isNodeDead())
      return new CardinalityDataStructureChannel(new ArrayList<Window>());
    
    WindowOperator logicalOp = (WindowOperator) operator.getSensornetOperator().getLogicalOperator();
    float to = logicalOp.getTo();
    float from = logicalOp.getFrom();
    float length = (to-from)+1;
    float slide;
    
    if(logicalOp.getTimeScope())
      slide = logicalOp.getTimeSlide();
    else
      slide = logicalOp.getRowSlide();
       
    String extent = logicalOp.getAttributes().get(1).toString();   
    if(slide >= length)
    {
      //tuples are parittioned
      CardinalityDataStructureChannel output = 
        new CardinalityDataStructureChannel(  inputs.getWindowsOfExtent(extent));
      return output;
    }
    else
    {
      return null;
    }
  }

  
  protected CardinalityDataStructureChannel selectCard(InstanceOperator operator,
                                                       CollectionOfPackets inputs) 
  throws OptimizationException
  {
    if(operator.isNodeDead())
      return new CardinalityDataStructureChannel(new ArrayList<Window>());
    String extent = operator.getSensornetOperator().getAttributes().get(1).toString();
    ArrayList<Window> inputWindows = inputs.getWindowsOfExtent(extent);
    ArrayList<Window> outputWindows = new ArrayList<Window>();
    
    for(int index = 0; index < inputWindows.size(); index++)
    {
      int windowCard = Math.round(inputWindows.get(index).getTuples() * operator.selectivity());
      outputWindows.add(new Window(windowCard,inputWindows.get(index).getWindowID()));
    }
    CardinalityDataStructureChannel output = new CardinalityDataStructureChannel(outputWindows);
    return output;
  }


  protected CardinalityDataStructureChannel RStreamCard(InstanceOperator operator,
                                                        CollectionOfPackets inputs) 
  throws OptimizationException
  {
    if(operator.isNodeDead())
      return new CardinalityDataStructureChannel(new ArrayList<Window>());
    String extent = operator.getExtent();
    return new CardinalityDataStructureChannel(inputs.getWindowsOfExtent(extent));
  }

  
  protected CardinalityDataStructureChannel aggerateCard(InstanceOperator operator,
                                                       CollectionOfPackets inputs,
                                                       long beta)
  throws OptimizationException
  {
    if(operator.isNodeDead())
      return new CardinalityDataStructureChannel(new ArrayList<Window>());
    
    String extent = operator.getExtent();
    ArrayList<Window> extentWindows = inputs.getWindowsOfExtent(extent);
    ArrayList<Window> outputWindows = new ArrayList<Window>();
    
    for(int index =1; index <= beta; index++ )
    {
      Window window = inputs.getWindow(index, extentWindows);
      if(window.getTuples() > 0)
        outputWindows.add(new Window(1,index));
      else
        outputWindows.add(new Window(0,index));
    }
    
    CardinalityDataStructureChannel output = new CardinalityDataStructureChannel(outputWindows);
    return output;
  }

 
  protected CardinalityDataStructureChannel acquireCard(InstanceOperator operator,
                                               CollectionOfPackets inputs) 
  throws OptimizationException
  {
    if(operator.isNodeDead())
      return new CardinalityDataStructureChannel(new ArrayList<Window>());
    
    int output = new Double(1 * operator.selectivity()).intValue();
    ArrayList<Window> windows = new ArrayList<Window>();
    windows.add(new Window(1,output));
    CardinalityDataStructureChannel out = new CardinalityDataStructureChannel(windows);
    //System.out.println(inputOperator.getID() + " outputCard= " + output);
    return out;
  }
  
 
  protected CardinalityDataStructureChannel joinCard(InstanceOperator operator,
                                                     CollectionOfPackets inputs, 
                                                     long beta)
      throws OptimizationException
  {
    ArrayList<String> doneExtents = new ArrayList<String>();
    Iterator<Node> inputIterator = operator.getInputsList().iterator();
    while(inputIterator.hasNext())
    {
      InstanceOperator cOp = (InstanceOperator) inputIterator.next();
      String currentExtent = null;
      if(cOp instanceof InstanceExchangePart)
      {
        InstanceExchangePart cOpex = (InstanceExchangePart) cOp;
        currentExtent = cOpex.getExtent();
      }
      else
        currentExtent = cOp.getExtent();
      if(!doneExtents.contains(currentExtent))
      {
        doneExtents.add(currentExtent);
      }
    }
    
    String extent1 = doneExtents.get(0);
    String extent2 = doneExtents.get(1);
    
    ArrayList<Window> windowsOfExtent1 = inputs.getWindowsOfExtent(extent1);
    ArrayList<Window> windowsOfExtent2 = inputs.getWindowsOfExtent(extent2);

    if(windowsOfExtent1 == null || windowsOfExtent2 == null)
      return  new CardinalityDataStructureChannel(new ArrayList<Window>());
    
    
    ArrayList<Window> output = new ArrayList<Window>();
    
    for(int index =0; index <= beta; index++)
    {
      Window extent1Window = inputs.getWindow(index, windowsOfExtent1);
      Window extent2Window = inputs.getWindow(index, windowsOfExtent2);
      int tuples = Math.round(extent1Window.getTuples() * extent2Window.getTuples() * operator.selectivity());
      output.add(new Window(tuples, index));
    }
    return  new CardinalityDataStructureChannel(output);
  }

  public CardinalityDataStructureChannel  model(InstanceOperator operator,
                                               CollectionOfPackets inputs, long beta)
  throws OptimizationException
  {
  //System.out.println("within operator " + operator.getID());
    if(operator.getSensornetOperator() instanceof SensornetAcquireOperator)
    {
      return acquireCard(operator, inputs);
    }
    else if(operator.getSensornetOperator() instanceof SensornetSingleStepAggregationOperator)
    {
      return aggerateCard(operator, inputs, beta);
    }
    else if(operator.getSensornetOperator() instanceof SensornetAggrEvalOperator)
    {
      return aggerateCard(operator, inputs, beta);
    }
    else if(operator.getSensornetOperator() instanceof SensornetAggrInitOperator)
    {
      return aggerateCard(operator, inputs, beta);
    }
    else if(operator.getSensornetOperator() instanceof SensornetAggrMergeOperator)
    {
      return aggerateCard(operator, inputs, beta);
    }
    else if(operator.getSensornetOperator() instanceof SensornetDeliverOperator)
    {
      return deliverModel(operator, inputs);
    }
    else if(operator.getSensornetOperator() instanceof SensornetNestedLoopJoinOperator)
    {
      return joinCard(operator, inputs, beta);
    }
    else if(operator.getSensornetOperator() instanceof SensornetProjectOperator)
    {
      InstanceOperator op = (InstanceOperator)(operator.getInstanceInput(0));
      return model(op, inputs,  beta);
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
      return new CardinalityDataStructureChannel(new ArrayList<Window>());
    }
  }
}
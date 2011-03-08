package uk.ac.manchester.cs.snee.compiler.costmodels;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetProjectOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetRStreamOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetSelectOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetWindowOperator;

public class CostModel
{
  private Agenda agenda;
  private RT routingTree;
  private InstanceDAF instanceDAF;
  private long epochDuration = 0;
  
  public CostModel()
  {
  }

  public void runCardinality() throws OptimizationException 
  {
	  InstanceOperator rootOperator = instanceDAF.getRoot();
	  epochDuration = agenda.getLength_bms(false) / agenda.getBufferingFactor();
	  
	  CardinalityStruct result = cardinalities(rootOperator);
	  float value = result.getCard();
    System.out.println("the cardinality of this query for agenda cycle is " + value);
  }
  
  private CardinalityStruct cardinalities(InstanceOperator operator) 
  throws OptimizationException
  {
    System.out.println("within operator " + operator.getID());
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
        InstanceOperator op = (InstanceOperator)(operator.getInput(0));
        CardinalityStruct input = cardinalities(op);
        return input;
      }
      else if(operator.getInstanceOperator() instanceof SensornetNestedLoopJoinOperator)
      {
        return joinCard(operator);
      }
      else if(operator.getInstanceOperator() instanceof SensornetProjectOperator)
      {
        InstanceOperator op = (InstanceOperator)(operator.getInput(0));
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
  
  private CardinalityStruct selectCard(InstanceOperator operator) 
  throws OptimizationException
  {
    InstanceOperator op = (InstanceOperator)(operator.getInput(0));
    CardinalityStruct input = cardinalities(op);
    CardinalityStruct output;
    if(input.isStream())
    {
      output = new CardinalityStruct(input.getCardOfStream() * operator.selectivity());
      System.out.println(operator.getID() + " inputCard= " + input);
      System.out.println(operator.getID() + " outputCard= " + output);
    }
    else if(input.isWindow())
    {
      output = new CardinalityStruct(input.getWindowCard() * operator.selectivity(), input.getWindowID());
      System.out.println(operator.getID() + " inputCard= " + input);
      System.out.println(operator.getID() + " outputCard= " + output);
    }
    else
    {
      output = new CardinalityStruct();
      for(int windowIndex = 0; windowIndex < input.getNoWindows(); windowIndex++)
      {
        float winCard = input.get(windowIndex).getWindowCard();
        output.addWindow(new CardinalityStruct(winCard * operator.selectivity(), input.get(windowIndex).getWindowID()));
      }
      System.out.println(operator.getID() + " inputCard= " + input);
      System.out.println(operator.getID() + " outputCard= " + output);  
    }
    return output;
  }

  private CardinalityStruct RStreamCard(InstanceOperator operator) 
  throws OptimizationException
  {
    InstanceOperator inputOperator = (InstanceOperator)(operator.getInput(0));
    CardinalityStruct input = cardinalities(inputOperator);
    CardinalityStruct output;
    if(input.isWindow())
    {
      output = new CardinalityStruct(input.getWindowCard());
    }
    else
    {
      output = new CardinalityStruct(input.getNoWindows() * input.get(0).getWindowCard());
    }
    
    System.out.println(operator.getID() + " inputCard= " + input);
    System.out.println(operator.getID() + " outputCard= " + output);
    return output; 
  }

  private CardinalityStruct acquireCard(InstanceOperator operator)
  {
    float output = (epochDuration / agenda.getAcquisitionInterval_ms()) * operator.selectivity();
    CardinalityStruct out = new CardinalityStruct(output);
    System.out.println(operator.getID() + " outputCard= " + output);
    return out;
  }

  private CardinalityStruct exchangeCard(InstanceOperator operator)
  throws OptimizationException
  {
    CardinalityStruct input;
    if(((InstanceExchangePart) operator).getPrevious() != null)//path
    {
      input =cardinalities(((InstanceExchangePart) operator).getPrevious());
    }
    else//hit new frag
    {
      input = cardinalities(((InstanceExchangePart) operator).getSourceFrag().getRootOperator());
    } 
    
    System.out.println(operator.getID() + " inputCard= " + input);
    System.out.println(operator.getID() + " outputCard= " + input);
    return input;
  }

  public CardinalityStruct windowCard(InstanceOperator op)
  throws OptimizationException
  {
    WindowOperator logicalOp = (WindowOperator) op.getInstanceOperator().getLogicalOperator();
    float to = logicalOp.getTo();
    float from = logicalOp.getFrom();
    float length = (to-from)+1;
    float slide;
    
    if(logicalOp.getTimeScope())
      slide = logicalOp.getTimeSlide();
    else
      slide = logicalOp.getRowSlide();
       
    InstanceOperator inputOperator = (InstanceOperator)(op.getInput(0));
    CardinalityStruct input = cardinalities(inputOperator);
      
    float noWindows;
    if(slide == 0)//now window, to stop infinity
      noWindows = 1;
    else
      noWindows = length / slide;
    
    float winCard = input.getCard();
    CardinalityStruct output = new CardinalityStruct();
    
    for(int windowIndex = 0; windowIndex < noWindows; windowIndex++)
    {
      output.addWindow(new CardinalityStruct(winCard, from + (windowIndex * slide)));
    }

    System.out.println(op.getID() + " inputCard= " + input);
    System.out.println(op.getID() + " outputCard= " + output);
    return output;
  }
  
  public CardinalityStruct aggerateCard(InstanceOperator operator)
  throws OptimizationException
  {
    CardinalityStruct output = null;
    ArrayList<CardinalityStruct> inputs = new ArrayList<CardinalityStruct>();
    for(int inputOperatorIndex = 0; inputOperatorIndex < operator.getInDegree(); inputOperatorIndex++)
    {
      InstanceOperator inputOperator = (InstanceOperator)(operator.getInput(0));
      inputs.add(cardinalities(inputOperator));
    }
    
    if(operator.getInDegree() == 1)
    {
      if(inputs.get(0).isWindow())
      {
        output = new CardinalityStruct(1, inputs.get(0).getWindowID());
      }
      else
      {
        output = new CardinalityStruct();
        for(int windowIndex = 0; windowIndex < inputs.get(0).getNoWindows(); windowIndex ++)
        {
          output.addWindow(new CardinalityStruct(1, inputs.get(0).getWindowID()));
        }
      }
    }
    else
    {
      if(inputs.get(0).isWindow())
      {
        output = new CardinalityStruct(1, inputs.get(0).getWindowID());
      }
      else
      {
        output = new CardinalityStruct();
        HashSet<Float> ids = new HashSet<Float>();
        Iterator<CardinalityStruct> inputsIterator = inputs.iterator();
        while(inputsIterator.hasNext())
        {
          CardinalityStruct input = inputsIterator.next();
          Iterator<CardinalityStruct> windowIterator = input.windowIterator();
          while(windowIterator.hasNext())
          {
            CardinalityStruct window = windowIterator.next();
            ids.add(window.getWindowID());
          }  
        }
        Iterator<Float> idIterator = ids.iterator();
        while(idIterator.hasNext())
        {
          output.addWindow(new CardinalityStruct(1, idIterator.next()));
        }
      }
    }
    float size = inputs.size();
    System.out.println(operator.getID() + " inputCard= " + size + " inputs each with "+ inputs.get(0).getCard());
    System.out.println(operator.getID() + " outputCard= " + output);
    return output;
  }
  
  public CardinalityStruct joinCard(InstanceOperator operator)
  throws OptimizationException
  {
    HashMap<Float, Float> leftWindowsCard = new HashMap<Float, Float>();
    HashMap<Float, Float> rightWindowsCard = new HashMap<Float, Float>();
    
    
    return null;
    /*
    ArrayList<Float> inputCards = new ArrayList<Float>();
    for(int index = 0; index < operator.getInDegree(); index++)
    {
      float card;
      InstanceOperator op = (InstanceOperator)(operator.getInput(index));
      card = cardinalities(op);
      inputCards.add(card);
    }
    float card = inputCards.get(0) * inputCards.get(1) * operator.selectivity();
    System.out.println(operator.getID() + " inputCard= " + card);
    System.out.println(operator.getID() + " outputCard= " + card);
    return card;*/
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
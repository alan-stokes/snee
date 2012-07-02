package uk.ac.manchester.cs.snee.manager.failednode.alternativerouter;

import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.graph.Edge;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.LinkCostMetric;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

public class HeuristicSet 
{
  private SecondNodeHeuristic chi;
  private FirstNodeHeuristic phi;
  private LinkMatrexChoiceHeuristic psi;
  private PenaliseNodeHeuristic omega;
  private SensibleHeuristic sens;
  
  private boolean successor = false;
  private LinkMatrexChoiceHeuristic choice;
  private HashMap<String, LinkMatrexChoiceHeuristic> edgeChoices = new HashMap<String, LinkMatrexChoiceHeuristic>();

  public HeuristicSet(SecondNodeHeuristic chi, FirstNodeHeuristic phi, 
                      LinkMatrexChoiceHeuristic psi, PenaliseNodeHeuristic omega,
                      SensibleHeuristic sens
                      ) 
  throws SNEEConfigurationException
  {
    this.chi = chi;
    this.phi = phi;
    this.psi = psi;
    this.omega = omega;
    this.sens = sens;
    successor = SNEEProperties.getBoolSetting(SNEEPropertyNames.WSN_MANAGER_SUCCESSOR);
  }
  
  
  public void setup(Topology workingTopology)
  {
    if(successor)
    {
      switch(psi)
      {
        case ENERGY:
          TreeMap<String, Edge> edges = workingTopology.getEdges();
          Iterator<String> keyIterator = edges.keySet().iterator();
          while(keyIterator.hasNext())
          {
            String key = keyIterator.next();
            Edge edge = edges.get(key);
            edgeChoices.put(edge.getID(), LinkMatrexChoiceHeuristic.ENERGY);
          }
        break;
        case LATENCY:
          edges = workingTopology.getEdges();
          keyIterator = edges.keySet().iterator();
          while(keyIterator.hasNext())
          {
            String key = keyIterator.next();
            Edge edge = edges.get(key);
            edgeChoices.put(edge.getID(), LinkMatrexChoiceHeuristic.LATENCY);
          }
        break;
        case RANDOM:
          choice = LinkMatrexChoiceHeuristic.ChoiceEnum();
          edges = workingTopology.getEdges();
          keyIterator = edges.keySet().iterator();
          while(keyIterator.hasNext())
          {
            String key = keyIterator.next();
            Edge edge = edges.get(key);
            edgeChoices.put(edge.getID(), choice);
          }
        break;
        case MIXED:
          edges = workingTopology.getEdges();
          keyIterator = edges.keySet().iterator();
          while(keyIterator.hasNext())
          {
            String key = keyIterator.next();
            Edge edge = edges.get(key);
            LinkMatrexChoiceHeuristic edgeChoice = LinkMatrexChoiceHeuristic.ChoiceEnum();
            edgeChoices.put(edge.getID(), edgeChoice);
          }
        break;
      }
    }
    else
    {
      switch(psi)
      {
        case ENERGY:
          TreeMap<String, Edge> edges = workingTopology.getEdges();
          Iterator<String> keyIterator = edges.keySet().iterator();
          while(keyIterator.hasNext())
          {
            String key = keyIterator.next();
            Edge edge = edges.get(key);
            edgeChoices.put(edge.getID(), LinkMatrexChoiceHeuristic.ENERGY);
          }
        break;
      }
    }
  }

  public HeuristicSet()
  {
  }

  public SecondNodeHeuristic getSecondNodeHeuristic()
  {
    return chi;
  }

  public void setSecondNodeHeuristic(SecondNodeHeuristic chi)
  {
    this.chi = chi;
  }

  public FirstNodeHeuristic getFirstNodeHeuristic()
  {
    return phi;
  }

  public void setFirstNodeHeuristic(FirstNodeHeuristic phi)
  {
    this.phi = phi;
  }

  public LinkMatrexChoiceHeuristic getLinkMatrexChoiceHeuristic()
  {
    return psi;
  }

  public void setLinkMatrexChoiceHeuristic(LinkMatrexChoiceHeuristic psi)
  {
    this.psi = psi;
  }

  public PenaliseNodeHeuristic getPenaliseNodeHeuristic()
  {
    return omega;
  }

  public void setPenaliseNodeHeuristic(PenaliseNodeHeuristic omega)
  {
    this.omega = omega;
  }
    
  public void setSensibleHeuristic(SensibleHeuristic sens)
  {
    this.sens = sens;
  }

  public SensibleHeuristic getSensibleHeuristic()
  {
    return sens;
  }
  
  public LinkCostMetric getEdgeChoice(String edgeID)
  {
    
    switch(edgeChoices.get(edgeID))
    {
      case ENERGY:
        return LinkCostMetric.ENERGY;
     /* case LATENCY:
        return LinkCostMetric.LATENCY;*/
      default:
        return  LinkCostMetric.RADIO_LOSS;
    }
  }
  
  public String toString()
  {
    String output = "";
    output = "firstChoice = " + phi + ": SecondChoice = " + chi + ": NetworkWeighting = " + psi + ": penalizeNodes = " + omega;
    return output;
  }
}

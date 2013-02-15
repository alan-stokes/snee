package uk.ac.manchester.cs.snee.manager.planner.successorrelation;

import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class FailedNodeData
{
  private Site node;
  private Double lifetime;
  private String successorID = null;
  
  public FailedNodeData(Site node, Double lifetime)
  {
    this.setNode(node);
    this.setLifetime(lifetime);
  }

  public void setNode(Site node)
  {
    this.node = node;
  }

  public Site getNode()
  {
    return node;
  }

  public void setLifetime(Double lifetime)
  {
    this.lifetime = lifetime;
  }

  public Double getLifetime()
  {
    return lifetime;
  }

  public void setSuccessorID(String successorID)
  {
    this.successorID = successorID;
  }

  public String getSuccessorID()
  {
    return successorID;
  }
}

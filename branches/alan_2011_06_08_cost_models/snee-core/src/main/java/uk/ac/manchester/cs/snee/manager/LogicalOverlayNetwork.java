package uk.ac.manchester.cs.snee.manager;

import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

public interface LogicalOverlayNetwork
{
  public abstract void updateTopology(Topology top);
}

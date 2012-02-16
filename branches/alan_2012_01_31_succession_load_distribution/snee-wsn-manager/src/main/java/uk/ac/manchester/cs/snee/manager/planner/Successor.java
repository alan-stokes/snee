package uk.ac.manchester.cs.snee.manager.planner;

import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;

public class Successor
{
  private SensorNetworkQueryPlan qep;
  private Integer agendaCount;
  
  public Successor(SensorNetworkQueryPlan qep, Integer agendaCount)
  {
    this.setQep(qep);
    this.setAgendaCount(agendaCount); 
  }

  public void setQep(SensorNetworkQueryPlan qep)
  {
    this.qep = qep;
  }

  public SensorNetworkQueryPlan getQep()
  {
    return qep;
  }

  public void setAgendaCount(Integer agendaCount)
  {
    this.agendaCount = agendaCount;
  }

  public Integer getAgendaCount()
  {
    return agendaCount;
  }
  
  
}

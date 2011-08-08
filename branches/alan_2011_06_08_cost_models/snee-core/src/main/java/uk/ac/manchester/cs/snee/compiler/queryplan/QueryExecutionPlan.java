package uk.ac.manchester.cs.snee.compiler.queryplan;

public interface QueryExecutionPlan
{

  public abstract QueryPlanMetadata getMetaData();

  /**
   * Gets the underlying DLAF.
   * @return
   */
  public abstract DLAF getDLAF();

  /**
   * Gets the underlying LAF.
   * @return
   */
  public abstract LAF getLAF();

  /**
   * Gets the query plan identifier.
   * @return
   */
  public abstract String getID();

  //delegate
  public abstract String getQueryName();

}
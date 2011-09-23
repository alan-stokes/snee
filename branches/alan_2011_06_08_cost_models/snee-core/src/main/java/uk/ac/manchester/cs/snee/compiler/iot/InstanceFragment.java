package uk.ac.manchester.cs.snee.compiler.iot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import java.util.logging.Logger;


import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;

public class InstanceFragment implements Serializable
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 66554258383409680L;

  /**
   * Logger for this class.
   */
  private static final Logger logger = Logger.getLogger(Fragment.class.getName());

  /**
   * Counter to assign unique id to different fragments.
   */
  public static int instanceFragmentCount = 0;
  public static int fragmentCount = 0;

  /**
   * Identifier of this fragment
   */
  protected int fragID;

  /**
   * The output exchange operator of this fragment (currently, there can only be one).
   */
  protected InstanceExchangePart parentExchange;

  /**
   * The input exchange operators of this fragment.
   */
  protected  ArrayList<InstanceExchangePart> childExchanges = 
    new ArrayList<InstanceExchangePart>();

  /**
   * The root operator of the fragment
   */
  protected InstanceOperator rootOperator = null;
  
  /**
   * The sensor network nodes to which this fragment has been allocated to run.
   */
  protected  Site site;

  /**
   * The list of sensor nodes where the operators in the fragment are required to run.
   * e.g., an Acquire operator will require certain data sources
   */
  public ArrayList<String> desiredSites = new ArrayList<String>();
  
  protected  HashSet<InstanceOperator> operators = new HashSet<InstanceOperator>();
  private InstanceFragment nextHigherFragment;
  private ArrayList<InstanceFragment> nextLowerFragment = new ArrayList<InstanceFragment>();
  
  /**
   * constructor
   */
  
  public InstanceFragment() 
  {
    instanceFragmentCount++;
    this.fragID = instanceFragmentCount;
  }
  
  public InstanceFragment(String fragID) 
  {
    this.fragID = Integer.parseInt(fragID);
  }
  
  
  public boolean isRemote(InstanceFragment other)
  {
	Integer thisSiteID = Integer.parseInt(this.site.getID());
	Integer otherSiteID = Integer.parseInt(other.site.getID());
    return !thisSiteID.equals(otherSiteID);
  }
  
  
  public InstanceFragment getNextHigherFragment()
  {
    return nextHigherFragment;
  }

  public void setNextHigherFragment(InstanceFragment nextHigherFragment)
  {
    this.nextHigherFragment = nextHigherFragment;
  }

  public ArrayList<InstanceFragment> getNextLowerFragment()
  {
    return nextLowerFragment;
  }
  
  public ArrayList<InstanceFragment> getChildFragments()
  {
    return nextLowerFragment;
  }

  public void addNextLowerFragment(InstanceFragment nextLowerFragment)
  {
    this.nextLowerFragment.add(nextLowerFragment);
  }
  
  public final HashSet<InstanceOperator> getInstanceOperators() 
  {
    return this.operators;
  }
  
  @SuppressWarnings("rawtypes")
  public final boolean containsInstanceOperatorType(final Class c) 
  {
    final Iterator<InstanceOperator> i = this.operators.iterator();
    while (i.hasNext()) 
    {
      final InstanceOperator op = i.next();
      if (c.isInstance(op)) 
      {
        return true;
      }
    }
    return false;
  }
  
  public final boolean isRecursive() 
  {
    boolean found = false;
    final Iterator<InstanceOperator> ops = this.operators.iterator();
    while (ops.hasNext()) 
    {
      if (ops.next().getSensornetOperator().isRecursive()) 
      {
        found = true;
        break;
      }
    }
    return found;
  }
  
  public final void addOperator(final InstanceOperator op) 
  {
    this.operators.add(op);
  }

  /**
   * @return the id of the fragment
   */
  public  String getID() {
    return new Integer(this.fragID).toString();
  }

  /**
   * Returns true if this is a leaf fragment
   */
  public final boolean isLeaf() {
    return (this.getChildFragments().size() == 0);
  }

  /**
   * Return the root operator of the fragment
   * (NB: The parent exchange is not considered to be within in the fragment;
   * this method returns the child of the parent exchange)
   * @return
   */
  public final InstanceOperator getRootOperator() {
    return this.rootOperator;
  }

  public final void setRootOperator(final InstanceOperator op) {
    this.rootOperator = op;
  }

  /**
   * @return  the child exchange operators of the fragment
   */
  public final ArrayList<InstanceExchangePart> getChildExchangeOperators() {
    return this.childExchanges;
  }

  /**
   * Get the parent fragments of this fragment
   * @return
   */
  public final InstanceFragment getParentFragment() {
    return nextHigherFragment;
  }

  /**
   * Get the sites on which all the child fragments have been placed
   * @return
   */
  public final HashSet<Site> getChildFragSites() {
  final HashSet<Site> childFragSites = new HashSet<Site>();
  final Iterator<InstanceFragment> fragIter = this.getChildFragments().iterator();
  while (fragIter.hasNext()) {
      final InstanceFragment f = fragIter.next();
      childFragSites.add(f.getSite());
  }
  return childFragSites;
  }

  /**
   * @return  the parent exchange operators of the fragment
   */
  public final InstanceExchangePart getParentExchangeOperator() {
    return this.parentExchange;
  }

  /**
   * Returns the ith child exchange operator
   * @param i   the positition of the child exchange operator
   * @return    the exchange operator
   */
  public final InstanceExchangePart getChildExchangeOperator(final int i) {
    return this.childExchanges.get(i);
  }

  /**
   * Returns the number of child exchange operators.
   * @return
   */
  public final int getNumChildExchangeOperators() {
    return this.childExchanges.size();
  }

  /**
   * @return the operators in the fragment
   */
  public final HashSet<InstanceOperator> getOperators() {
    return this.operators;
  }

  public final int[] getSourceNodes() {
    return this.getRootOperator().getSourceSites();
  }

  public final boolean containsOperatorType(@SuppressWarnings("rawtypes") final Class c) {
  final Iterator<InstanceOperator> i = this.operators.iterator();
  while (i.hasNext()) {
      final InstanceOperator op = i.next();
      if (c.isInstance(op)) {
    return true;
      }
  }
  return false;
  }

  public final boolean isLocationSensitive() {
  boolean found = false;
  final Iterator<InstanceOperator> ops = this
    .operatorIterator(TraversalOrder.PRE_ORDER);
  while (ops.hasNext()) {
      if (ops.next().getSensornetOperator().getLogicalOperator().isLocationSensitive()) {
    found = true;
    break;
      }
  }
  return found;
  }

  public final boolean isAttributeSensitive() {
boolean found = false;
final Iterator<InstanceOperator> ops = this
  .operatorIterator(TraversalOrder.PRE_ORDER);
while (ops.hasNext()) {
    if (ops.next().getSensornetOperator().getLogicalOperator().isAttributeSensitive()) {
  found = true;
  break;
    }
}
return found;
  }

  /**
   * 
   * @return True if the fragment ends in a deliver and therefor does not output tuples to a tray.
   */
  public final boolean isDeliverFragment() {
    if (this.getRootOperator().getSensornetOperator() instanceof DeliverOperator) {
      return true;
    }
    return false;
  }

  /**
   * @return the sensor network nodes this fragment has been allocated to
   */
  public final Site getSite() {
    return this.site;
  }

  /**
   * Helper method to implement the operator iterator.
   * @param op The current operator being visited
   * @param opList The list of operators being generated (the result).
   * @param traversalOrder The order in which to traverse the operators.
   */
  private void doOperatorIterator(final InstanceOperator op,
    final ArrayList<InstanceOperator> opList, 
    final TraversalOrder traversalOrder) {

  if (traversalOrder == TraversalOrder.PRE_ORDER) {
      opList.add(op);
  }

  for (int n = 0; n < op.getInDegree(); n++) {
      if (this.operators.contains(op.getInput(n))) {
        this.doOperatorIterator((InstanceOperator)op.getInput(n), 
            opList, traversalOrder);
      }
  }

  if (traversalOrder == TraversalOrder.POST_ORDER) {
      opList.add(op);
  }
  }

  /**
   * Iterator to traverse the operator tree.
   * The structure of the routing tree may not be modified during iteration
   * @param traversalOrder (constants defined in QueryPlan class)
   * @return an iterator of the operator tree in the fragment
   */
  public final Iterator<InstanceOperator> operatorIterator(
      final TraversalOrder traversalOrder) {
  final ArrayList<InstanceOperator> opList =
      new ArrayList<InstanceOperator>();
  logger.finest("root =" + this.getRootOperator());
  this.doOperatorIterator(this.getRootOperator(), opList, traversalOrder);
  logger.finest("done");
  return opList.iterator();
  }

  //get lowest operator
  public InstanceOperator getLowestOperator()
  {
	  Iterator<InstanceOperator> iterator = this.operatorIterator(TraversalOrder.POST_ORDER);
	  return iterator.next();
  }
 
  /** 
   * Calculates the physical size of the state of the operators in this 
   * fragment.  Does not include the size of the exchange components 
   * including the consumers and producers.  Does not include the size 
   * of the code itself.
   * 
   * @return Sum of the cost of each of the operators
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   */
  public final long getDataMemoryCost(final Site node, final DAF daf)
  throws SchemaMetadataException, TypeMappingException, OptimizationException {
long total = 0;
final Iterator<InstanceOperator> ops = this
  .operatorIterator(TraversalOrder.PRE_ORDER);
while (ops.hasNext()) {
    final InstanceOperator op = ops.next();
    logger.finest("op=" + op);
    total += op.getDataMemoryCost(node, daf);
}
logger.finest("done");
return total;
  }

//  public final AlphaBetaExpression getMemoryExpression(final Site node,
//    final DAF daf) {
//    return new AlphaBetaExpression(this.getDataMemoryCost(node, daf), 0);
//  }

  /**
   * Calculates the time cost for a single evaluation of all the operators 
   * in this fragment.  The time cost is based on the maximum cardinality 
   * not the average cardinality.  Does not include the time of the exchange 
   * components including the consumers and producers
   * 
   * Based on the time estimates provided in the OperatorsMetaData file.
   * Includes the cost of copying the tuples to the tray.
   * 
   * @param node The site the operator has been placed on
   * @param daf The distributed-algebraic form of the corresponding 
   * operator tree.
   * @return Sum of the cost of each of the operators
   * @throws OptimizationException 
   */
  public final double getTimeCost(final Site node, final DAF daf, 
  CostParameters costParams) throws OptimizationException {
  long total = 0;
  final Iterator<InstanceOperator> ops = this
    .operatorIterator(TraversalOrder.PRE_ORDER);
  while (ops.hasNext()) {
      final InstanceOperator op = ops.next();
      logger.finest("op: " + op.toString());
      final double temp = op.getTimeCost(
          CardinalityType.PHYSICAL_MAX, node, daf);
      logger.finest("ops TimeCost =" + temp);
      total += temp;
  }
  if (!this.isDeliverFragment()) {
      final int cardinality = this.getRootOperator()
        .getCardinality(CardinalityType.PHYSICAL_MAX, node, daf);
      total += cardinality * costParams.getCopyTuple();
  }
  return total;
  }

  public final void setParentExchange(final InstanceExchangePart p) {
this.parentExchange = p;
  }

  public final void addChildExchange(final InstanceExchangePart c) {
if (!this.childExchanges.contains(c)) {
    this.childExchanges.add(c);
}
  }

  /**
   * Adds a sensor network node to execute fragment
   * They are added in order (for good looking display purposes)
   * @param n
   */
  public final void setSite(final Site newSite) 
  {
    this.site = newSite;
  }

  /**
   * Resets the fragment counter (for use when a new query plan is instantiated)
   *
   */
  public static void resetFragmentCounter() {
    instanceFragmentCount = 0;
  }

  public final void addDesiredSite(final Site n) {
    this.desiredSites.add(n.getID());
  }

  public final void addDesiredSites(final int[] nindices, final RT routingTree) {
    for (int element : nindices) {
      this.addDesiredSite((Site) routingTree.getSite(element));
    }
  }

  public static int getFragmentCount()
  {
    return fragmentCount;
  }

  public static void setFragmentCount(int fragmentCount)
  {
    InstanceFragment.fragmentCount = fragmentCount;
  }
  
  public final ArrayList<String> getDesiredSites() {
return this.desiredSites;
  }

  public final String getDesiredSitesString() {
final Iterator<String> siteIter = this.desiredSites.iterator();
boolean first = true;
final StringBuffer s = new StringBuffer();

while (siteIter.hasNext()) {
    final String siteID = siteIter.next();
    s.append(siteID);
    if (first) {
  first = false;
    } else {
  s.append(",");
    }
}

return s.toString();
  }

  /**
   * 
   * @return
   */
  public final int getNumChildFragments() {
return this.getChildFragments().size();
  }
  
  public int getNumSites() {
    return 1;
  }


  public String getFragID()
  {
    // TODO Auto-generated method stub
    return null;
  }
 
 
}

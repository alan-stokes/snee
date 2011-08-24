/****************************************************************************\ 
*                                                                            *
*  SNEE (Sensor NEtwork Engine)                                              *
*  http://code.google.com/p/snee                                             *
*  Release 1.0, 24 May 2009, under New BSD License.                          *
*                                                                            *
*  Copyright (c) 2009, University of Manchester                              *
*  All rights reserved.                                                      *
*                                                                            *
*  Redistribution and use in source and binary forms, with or without        *
*  modification, are permitted provided that the following conditions are    *
*  met: Redistributions of source code must retain the above copyright       *
*  notice, this list of conditions and the following disclaimer.             *
*  Redistributions in binary form must reproduce the above copyright notice, *
*  this list of conditions and the following disclaimer in the documentation *
*  and/or other materials provided with the distribution.                    *
*  Neither the name of the University of Manchester nor the names of its     *
*  contributors may be used to endorse or promote products derived from this *
*  software without specific prior written permission.                       *
*                                                                            *
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS   *
*  IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, *
*  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR    *
*  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR          *
*  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,     *
*  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,       *
*  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR        *
*  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF    *
*  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING      *
*  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS        *
*  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.              *
*                                                                            *
\****************************************************************************/
package uk.ac.manchester.cs.snee.metadata.source.sensornet;

import java.util.HashSet;
import java.util.Iterator;
import java.util.logging.Logger;

import uk.ac.manchester.cs.snee.common.graph.NodeImplementation;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetExchangeOperator;

public class Site extends NodeImplementation implements Comparable{

    /**
     * Logger for this class.
     */
	private static Logger logger = 
		Logger.getLogger(Site.class.getName());
	
    /**
     * The RAM available on a node, in bytes
     */
    long ram;

    /**
     * The energy stock available for the site, in milliJoules 
     */
    long energyStock;

    /**
     * The fragments which have been placed on a node
     */
    HashSet<Fragment> fragments = new HashSet<Fragment>();

    /**
     * The exchange components which have been placed on a node
     */
    HashSet<ExchangePart> exchangeComponents = new HashSet<ExchangePart>();

    /**
     * The instance exchange components which have been placed on a node
     * used within cost model calculations
     */
    HashSet<InstanceExchangePart> instanceExchangeComponents = new HashSet<InstanceExchangePart>();

	/**
     * Flag which tracks whether the node is a source node (i.e., acquires sensor readings)
     */
    boolean source = false;
    
    /**
     * Count the number of sources in the sub-tree routed at this site in the routing tree.
     */
    int numSources = -1;
    
    /**
     * Constructor for a sensor network node
     * @param id
     */
    public Site(final String id) {
    	super(id);
    }

    public Site(Site model) {
    	super(model.id);
		this.ram = model.ram;
		this.energyStock = model.energyStock;
		this.source = model.source;
		this.numSources = model.numSources;
		//this method does not link fragments or exchange components,
		//needs to be done later
	}

	public Site getChild(final int i) {
	return (Site) super.getInput(i);
    }

    public void setEnergyStock(final long energyVal) {
	this.energyStock = energyVal;
    }

    public long getEnergyStock() {
	return this.energyStock;
    }

    public void setRAM(final long ramVal) {
	this.ram = ramVal;
    }

    /**
     * @return the amount of memory available on the node for this query
     */
    public long getRAM() {
	return this.ram;
    }

    /**
     * Checks if a fragment has been allocated on a particular node
     * @param frag
     * @return
     */
    public boolean hasFragmentAllocated(final Fragment frag) {
	return this.getFragments().contains(frag);
	
    }

    /**
     * @return	the fragments which have been placed on the node
     */
    public HashSet<Fragment> getFragments() {
    	return this.fragments;
    }

    /**
     * @return	the exchange components which have been plaecd on a node
     */
    public HashSet<ExchangePart> getExchangeComponents() {
	return this.exchangeComponents;
    }

    
    public void addFragment(final Fragment frag) {
	//TODO: here we should check resource availability, 
	//and throw an error if insufficient resources
    	this.fragments.add(frag);
    }

    public void addExchangeComponent(final ExchangePart exchComp) {
    	this.exchangeComponents.add(exchComp);
    }

//    public String getFragmentsString() {
//		final Iterator<Fragment> fragIter = this.fragments.iterator();
//		final StringBuffer s = new StringBuffer();
//		boolean first = true;
//	
//		while (fragIter.hasNext()) {
//		    final Fragment f = fragIter.next();
//		    if (!first) {
//			s.append(",");
//		    } else {
//			first = false;
//		    }
//		    s.append("F" + f.getID());
//		}
//	
//		return s.toString();
//    }
    
//    public String getExchangeComponentsString() {
//	final Iterator<ExchangePart> exchCompIter = this.exchangeComponents
//		.iterator();
//	final StringBuffer s = new StringBuffer();
//	boolean first = true;
//
//	while (exchCompIter.hasNext()) {
//	    final ExchangePart exchComp = exchCompIter.next();
//	    if (!first) {
//		s.append(",\\n");
//	    } else {
//		first = false;
//	    }
//	    s.append(exchComp.toString());
//	}
//
//	return s.toString();
//    }

//    // does not populate fragments or exchangeComponents
//    @Override
//    public Site shallowClone() {
//		final Site clonedNode = new Site(this.id);
//	
//		clonedNode.ram = this.ram;
//		clonedNode.energyStock = this.energyStock;
//		clonedNode.source = this.source;
//		clonedNode.numSources = this.numSources;
//		
//		//this method does not link fragments or exchange components,
//		//needs to be done later
//	
//		return clonedNode;
//    }
    
    /**
     * Returns whether this site is a source site.
     * @return
     */
    public boolean isSource() {
    	return this.source;
    }
    
    /**
     * Setter method to specify whether site is a source site.
     */
    public void setIsSource(boolean isSource) {
    	this.source = isSource;
    }

    /**
     * Update the numSources attribute at this site and all children of this site.
     */
    public void updateNumSources() {
    	this.numSources = 0;
    	if (this.isSource()) {
    		this.numSources++;
    	}
    	for (int i=0; i<this.getInDegree(); i++) {
    		Site s = this.getChild(i);
    		s.updateNumSources();
    		this.numSources += s.numSources;
    	}
    }
    
    /**
     * Getter method for the numSources attribute.
     * @return numSources
     */
    public int getNumSources() {
    	return this.numSources;
    }

/**
     * Remove the given exchange components for this site.
     * @param exchOp The exchange operator whose exchange components are to be removed.
     */
	public void removeExchangeComponents(SensornetExchangeOperator exchOp) {
		HashSet<ExchangePart> exchCompsToRemove = new HashSet<ExchangePart>();		
		Fragment sourceFrag = exchOp.getSourceFragment();
		Fragment destFrag = exchOp.getDestFragment();
		
		//First identify the exchange components to be removed
		//We assume that an exchange is uniquely identified by its source fragment and 
		//destination fragment, which seems to be a reasonable assumption
		Iterator<ExchangePart> exchCompIter = this.exchangeComponents.iterator();
		while (exchCompIter.hasNext()) {
			ExchangePart exchComp = exchCompIter.next();
			if (exchComp.getSourceFrag()==sourceFrag && 
					exchComp.getDestFrag()==destFrag) {
				exchCompsToRemove.add(exchComp);
			}
		}
		
		//Now remove the exchange components which were previously identified
		exchCompIter = exchCompsToRemove.iterator();
		while (exchCompIter.hasNext()) {
			ExchangePart exchComp = exchCompIter.next();
			this.exchangeComponents.remove(exchComp);
		}
	}

	
	/**
	 * Removes a given fragment which has been placed on this site.
	 * @param frag The fragment to be removed
	 */
	public void removeFragment(Fragment frag) {
		this.fragments.remove(frag);
	}

    boolean isDead; 
    public boolean isDeadInSimulation()
    {
      return isDead;
    }
    public void setisDead(boolean isDead)
    {
      this.isDead = isDead;
    }
    
    public boolean addInstanceExchangePart(InstanceExchangePart exchangePart)
    {
      return instanceExchangeComponents.add(exchangePart);
    }
    
    public HashSet<InstanceExchangePart> getInstanceExchangeComponents()
    {
		  return instanceExchangeComponents;
	  }

    public void clearInstanceExchangeComponents()
    {
      this.instanceExchangeComponents.clear();
    }
    
    @Override
    public int compareTo(Object arg0)
    {
      Site other = (Site) arg0;
      if(this.id.equals(other.id))
        return 0;
      else
        return -1;
    }

    public void setNoSources(int newNumSources)
    {
      this.numSources = newNumSources;    
    }

    public void clearExchangeComponents()
    {
      this.exchangeComponents.clear();      
    }

//    /**
//     * Remove the given exchange components for this site.
//     * @param exchOp The exchange operator whose exchange components are to be removed.
//     */
//	public void removeExchangeComponents(ExchangeOperator exchOp) {
//		HashSet<ExchangePart> exchCompsToRemove = new HashSet<ExchangePart>();		
//		Fragment sourceFrag = exchOp.getSourceFragment();
//		Fragment destFrag = exchOp.getDestFragment();
//		
//		//First identify the exchange components to be removed
//		//We assume that an exchange is uniquely identified by its source fragment and 
//		//destination fragment, which seems to be a reasonable assumption
//		Iterator<ExchangePart> exchCompIter = this.exchangeComponents.iterator();
//		while (exchCompIter.hasNext()) {
//			ExchangePart exchComp = exchCompIter.next();
//			if (exchComp.getSourceFrag()==sourceFrag && 
//					exchComp.getDestFrag()==destFrag) {
//				exchCompsToRemove.add(exchComp);
//			}
//		}
//		
//		//Now remove the exchange components which were previously identified
//		exchCompIter = exchCompsToRemove.iterator();
//		while (exchCompIter.hasNext()) {
//			ExchangePart exchComp = exchCompIter.next();
//			this.exchangeComponents.remove(exchComp);
//		}
//	}
//
//	
//	/**
//	 * Removes a given fragment which has been placed on this site.
//	 * @param frag The fragment to be removed
//	 */
//	public void removeFragment(Fragment frag) {
//		this.fragments.remove(frag);
//	}
//
//
//    /**
//     * Calculates the total data memory cost of the fragments.
//     * @param daf The query plan DAF
//     * @return Amount of RAM used by the fragments.
//     */
//    public final int getTotalFragmentDataMemoryCost(final DAF daf) {
//        int totalFragmentDataMemoryCost = 0;
//        final Iterator<Fragment> fragIter = 
//        	this.getFragments().iterator();
//        while (fragIter.hasNext()) {
//        	final Fragment fragment = fragIter.next();
//        	totalFragmentDataMemoryCost += fragment.getDataMemoryCost(
//        			this, daf);
//        }
//        logger.finest("totalFragmentDataMemoryCost ="+totalFragmentDataMemoryCost);
//        return totalFragmentDataMemoryCost;
//    }
//
//    /**
//     * Calculates the data memory cost of the exchanges for each Beta.
//     * Excludes overheads that do not change when the buffering factor changes. 
//     * @param daf The query plan DAF
//     * @return Amount of RAM used by the exchanges.
//     */
//    public final int getExchangeComponentsDataMemoryCostPerBeta(
//    		final DAF daf) { 
//    	int totalExchangeComponentsDataMemoryCost = 0;
//
//    	final Iterator<ExchangePart> comps = 
//	    	getExchangeComponents().iterator();
//    	while (comps.hasNext()) {
//    		final ExchangePart comp = comps.next();
//    		totalExchangeComponentsDataMemoryCost += comp
//				.getDataMemoryCostPerBeta(this, daf);
//    	}
//    	logger.finest("totalExchangeComponentsDataMemoryCost =" + totalExchangeComponentsDataMemoryCost);
//    	return totalExchangeComponentsDataMemoryCost;
//    }
//    
//    /**
//     * Calculates the data memory cost of the exchanges for each Beta.
//     * Excludes overheads that do not change when the buffering factor changes. 
//     * @param daf The query plan DAF
//     * @return Amount of RAM used by the exchanges.
//     */
//    public final int getExchangeComponentsOverheadMemoryCost(
//    		final DAF daf) { 
//    	int totalExchangeComponentsOverheadMemoryCost = 0;
//
//    	final Iterator<ExchangePart> comps = 
//	    	getExchangeComponents().iterator();
//    	while (comps.hasNext()) {
//    		final ExchangePart comp = comps.next();
//    		totalExchangeComponentsOverheadMemoryCost += comp
//				.getDataMemoryCost(this, daf);
//    	}
//    	logger.finest("totalExchangeComponentsOverheadMemoryCost =" + totalExchangeComponentsOverheadMemoryCost);
//    	return totalExchangeComponentsOverheadMemoryCost;
//    }
//    
//    /**
//     * Calculates the overhead data memory.
//     * Includes the RAM for the timers, radio and Query plan.
//     * @return Amount of RAM used by the normal overhead.
//     */
//    public final int getOverheadDataMemoryCost() { 
//   		int generalOverhead = MemoryCostParameters.TimerOverhead 
//   			+ MemoryCostParameters.StdControlOverhead;
//    	
//   		//nextDelta, agendaTime, busyUntil, evalTime;
//   		int queryPlanM = 4 * MemoryCostParameters.IntegerSize; 
//   		//AgendaTimer, syncTimer
//   		queryPlanM += MemoryCostParameters.Timer * 2;
//   	
//   		int total = generalOverhead + queryPlanM; 
//
//   		if (this.exchangeComponents.size() > 0) {
//   			int radioOverheads = MemoryCostParameters.RadioOverHead;
//   			if (this.fragments.size() == 0) {
//   				radioOverheads = MemoryCostParameters.RelayOverhead;
//   			}
//   			total += radioOverheads;
//   		}
//   		
//   		if (Settings.NESC_LED_ENABLE) {
//   			total += MemoryCostParameters.Leds;
//   		}
//   		
//   		logger.finest("Overhead ="+total);
//   		return total;
//    }    
//    
//    /**
//     * Calculates the data memory cost of this site for this Beta.
//     * Excludes overheads that do not change when the buffering factor changes. 
//     * @param daf The query plan DAF
//     * @param beta Buffering Factor
//     * @return Amount of RAM used on the site.
//     */
//    public final long getDataMemoryCost(final DAF daf, final long beta) {
//    	logger.finest(id);
//    	long total = getTotalFragmentDataMemoryCost(daf) 
//    	  + getExchangeComponentsOverheadMemoryCost(daf)
//    	  + getExchangeComponentsDataMemoryCostPerBeta(daf) * beta
//    	  + getOverheadDataMemoryCost();
//    	logger.finest(new Long(total).toString());
//    	return total;
//    }

}

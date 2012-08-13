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
package uk.ac.manchester.cs.snee.compiler.iot;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePartType;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetExchangeOperator;

/**
 * Class to represent exchange operator parts (namely: producer, relay, consumer).
 * 
 * Each instance of the source fragment of the exchange is "wired" to one instance of 
 * the destination fragment of the exchange.
 * 
 * For a given wiring, both the source fragment and the destination fragment may be 
 * on the same site, in which case the wiring comprises of an exchange producer (from the 
 * source fragment instance) directly connected to an exchange consumer (from the 
 * destination fragment instance).
 * 
 * If they are on different sites, a relay part is placed along each site along the
 * multi-hop path.
 */
public class InstanceExchangePart extends InstanceOperator{

   /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -1869780661146233636L;

  @SuppressWarnings("unused")
  private static final Logger logger = Logger.getLogger(InstanceExchangePart.class.getName());

    /**
     * The source fragment of the exchange.
     */
    private InstanceFragment sourceFrag;

    /**
     * The source site of this exchange operator part.
     */
    private Site sourceSite;

    /**
     * The destination site of this exchange operator part.
     */
    private InstanceFragment destFrag;

    /**
     * The destination site of this exchange operator part.
     */
    private Site destSite;

    /**
     * The type of this exchange operator part.
     */
    private ExchangePartType partType;

    /**
     * Flag to indicate whether this exchange operator part is remote or not.
     */
    private boolean isRemote;

    /**
     * The previous part of the exchange in the path.
     * This is null for producers by definition.
     */
    private InstanceExchangePart prev;

    /**
     * The next part of the exchange in the path.
     * This is null for consumers by definition.
     */
    private InstanceExchangePart next;

    /***
     * used to represent the part in operator instance tree diagrams
     */
    private String id;
    
    /**
     * used to represent a cloning from a preivous position(overlay)
     */
    private String previousId = null;

    /**
     * Constructor for exchange part.
     * @param sourceFrag
     * @param sourceSite
     * @param destFrag
     * @param destSite
     * @param currentSite
     * @param partType
     * @param isRemote
     * @param prev
     * @param costs 
     * @throws SchemaMetadataException 
     * @throws SNEEException 
     */
    public InstanceExchangePart(final InstanceFragment sourceFrag, final Site sourceSite,
	    final InstanceFragment destFrag, final Site destSite,
	    final Site currentSite, final ExchangePartType partType,
	    final boolean isRemote, final InstanceExchangePart prev, CostParameters costs) throws SNEEException, SchemaMetadataException {

		this.sourceFrag = sourceFrag;
		this.sourceSite = sourceSite;
		this.destFrag = destFrag;
		this.destSite = destSite;
		this.setSite(currentSite);
		this.partType = partType;
		this.isRemote = isRemote;
		this.setInstanceOperator(new SensornetExchangeOperator(costs));
		this.id = "F" + sourceFrag.getID() + "_S" + this.getSite().getID() + "_" + partType.toString().toLowerCase();
	
		//link components in a path
		this.prev = prev;
		if (partType != ExchangePartType.PRODUCER) {
		    prev.next = this;
		}
		if (partType == ExchangePartType.CONSUMER) {
		    this.next = null;
	}

	currentSite.addInstanceExchangePart(this);
 }
  
    public String getID() {
		return id;
	}
    
  public void regenerateID()
  {
    if(previousId == null)
    {
      previousId = this.id;
      this.id = "F" + sourceFrag.getID() + "_S" + this.getSite().getID() + "_" + partType.toString().toLowerCase();
    }
    else
    {
      this.id = "F" + sourceFrag.getID() + "_S" + this.getSite().getID() + "_" + partType.toString().toLowerCase() + "(C)";
    }
  }

	public static int computeTuplesPerMessage(final int tupleSize, 
    CostParameters costParams) throws OptimizationException {
    	Logger logger = Logger.getLogger(InstanceExchangePart.class.getName());
    	
    	int maxMessagePayloadSize = costParams.getMaxMessagePayloadSize();
    	int payloadOverhead = costParams.getPayloadOverhead();
    	logger.trace("maxMessagePayloadSize="+maxMessagePayloadSize);
    	logger.trace("payloadOverhead="+payloadOverhead);
    	logger.trace("tupleSize="+tupleSize);
    	
		final int numTuplesPerMessage = (int) Math.floor(maxMessagePayloadSize - payloadOverhead)
			/ (tupleSize);
		
		logger.trace("numTuplesPerMessage="+numTuplesPerMessage);
		if (numTuplesPerMessage <= 0) {
			String msg = "Unable to fit whole tuple in packet.";
			logger.warn(msg);
			throw new OptimizationException(msg);
		}
		return numTuplesPerMessage;
    }

    @Override
	public final String toString() {
		final StringBuffer s = new StringBuffer();
		s.append("f" + this.sourceFrag.getID());
		s.append("n" + this.sourceSite.getID());
		s.append("f" + this.destFrag.getID());
		s.append("n" + this.destSite.getID());
	
		if (this.partType == ExchangePartType.PRODUCER) {
		    s.append("P");
		} else if (this.partType == ExchangePartType.RELAY) {
		    s.append("R" + this.getSite().getID());
		} else if (this.partType == ExchangePartType.CONSUMER) {
		    s.append("C");
		}
	
		if (this.isRemote) {
		    s.append("*");
		}

		return s.toString();
    }

    /**
     * Returns the type of component of this exchange part.
     * @return
     */
    public final ExchangePartType getComponentType() {
    	return this.partType;
    }

    
    /**
     * Returns the current site this exchPart is on.
     * @return
     */
    public final Site getCurrentSite() {
    	return this.getSite();
    }

    
    /**
     * Returns the source fragment of the exchange operator.
     * @return
     */
    public final InstanceFragment getSourceFrag() {
    	return this.sourceFrag;
    }

    
    /**
     * Returns the source fragment id of the exchange operator.
     * @return The source fragment id.
     */
    public final String getSourceFragID() {
    	return this.sourceFrag.getID();
    }

    /**
     * Returns the source site of the path.
     * @return The source site.
     */
    public final Site getSourceSite() {
    	return this.sourceSite;
    }
    
    
    /**
     * Returns the source site id of the path.
     * @return The source site id.
     */
    public final String getSourceSiteID() {
    	return this.sourceSite.getID();
    }

    
    /**
     * Returns the destination fragment of the whole exchange operator.
     * @return The destination fragment.
     */
    public final InstanceFragment getDestFrag() {
    	return this.destFrag;
    }

    
    /**
     * Returns the destination fragment id of the whole exchange operator.
     * @return The destination fragment id.
     */
    public final String getDestFragID() {
    	return this.destFrag.getID();
    }

    
    /**
     * Returns the destination site id of the path.
     * @return The destination site id.
     */
    public final String getDestSiteID() {
    	return this.destSite.getID();
    }

    
    /**
     * Returns the destination site of this path.
     * @return The destination site.
     */
    public final Site getDestSite() {
    	return this.destSite;
    }

    /**
     * Returns the current site that this exchange part is on.
     * @return The current site id.
     */
    public final String getCurrentSiteID() {
    	return this.getSite().getID();
    }

    /**
     * 
     * @return
     */
    public final boolean isRemote() {
    	return this.isRemote;
    }

    /**
     * Get the previous exchange component in the chain.
     * @return
     */
    public final InstanceExchangePart getPrevious() {
    	return this.prev;
    }

    public void setPrev(InstanceExchangePart prev)
    {
      this.prev = prev;
    }
    
    
    /**
     * Get the next exchange component in the chain.
     * @return
     */
    public final InstanceExchangePart getNext() {
    	return this.next;
    }

    /**
     * Calculates the physical size of the state of this exchange 
     * for a single evaluation.
     * Includes the size of the tray.
     * 
     * Does not include the size of the code itself.
     * @param site Physical mote on which this operator has been placed.
     * @param daf Distributed query plan this operator is part of.
     * @return Memory cost of this exchange.
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     * @throws OptimizationException 
     */
    public final int getDataMemoryCost(final Site site, final DAF daf) throws OptimizationException, SchemaMetadataException, TypeMappingException {
    	InstanceOperator root = this.sourceFrag.getRootOperator();
 		return root.getCardinality(CardinalityType.MAX, this.sourceSite, daf)
			* root.getSensornetOperator().getPhysicalTupleSize();
    }

//    /**
//     * Calculates the physical size of the state of this exchange 
//     * that vary with bufferingFactor for a single evaluation.
//     * Includes the variable size of the tray.
//     * 
//     * Does not include the size of the code itself.
//     * @param site Physical mote on which this operator has been placed.
//     * @param daf Distributed query plan this operator is part of.
//     * @return Memory cost of this exchange.
//     */
//    public final long getDataMemoryCostPerBeta(final Site site, final DAF daf) {
//    	Operator root = this.sourceFrag.getRootOperator();
// 		return root.getCardinality(CardinalityType.MAX, this.sourceSite, daf)
//			* root.getPhysicalTupleSize();
//    }

//    //Memory is not a constant as each output is saved in seperate subtrays. 
//    public final AlphaBetaExpression getMemoryBetaExpression(final Site site,
//	    final DAF daf) {
//    	return new AlphaBetaExpression(this.getDataMemoryCost(site, daf), 0);
//    }

    /**
     * Calculates the number of packets sent in one one evaluation.
     * 
     * @param daf Distributed query plan this operator is part of.
     * @param bufferingFactor BufferingFactor.
     * @return Number of packets sent. 
     * @throws OptimizationException 
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     */
    public final int packetsPerTask(final DAF daf, 
    final long bufferingFactor, CostParameters costParams) 
    throws OptimizationException, SchemaMetadataException, TypeMappingException {
    	InstanceOperator root = sourceFrag.getRootOperator();
    	final long tuples
    		= root.getCardinality(CardinalityType.MAX, sourceSite, daf) 
    		* bufferingFactor;
		final int tupleSize = root.getSensornetOperator().getPhysicalTupleSize();
		final int tuplesPerPacket = computeTuplesPerMessage(tupleSize, costParams);
		return (int) Math.ceil(tuples / ((float) tuplesPerPacket));
    }

//    /**
//     * Calculates the number of packets sent in one evaluation.
//     * 
//     * @param card CardinalityType The type of cardinality to be considered.
//     * @param daf Distributed query plan this operator is part of.
//	 * @param round Defines if rounding reserves should be included or not
//     * @return Number of packets sent. 
//     */
//    public final AlphaBetaExpression packetsPerTask(final CardinalityType card, 
//    		final DAF daf, final boolean round) {
//    	Operator root = sourceFrag.getRootOperator();
//    	AlphaBetaExpression result = root.getCardinality(
//    		card, sourceSite, daf, round).clone();
// 		final int tupleSize = root.getPhysicalTupleSize();
//		final float tuplesPerPacket = computeTuplesPerMessage(tupleSize);
//		result.divideBy(tuplesPerPacket);
//
//		if (round) {
//			//The last packet may have to be filled.
//			//Adjust by the max empty packet that may be sent.
//			//(All but one tuple empty)
//			final AlphaBetaTerm  fillLastPacket = 
//				new AlphaBetaTerm(tuplesPerPacket - 1);
//			fillLastPacket.divideBy(tuplesPerPacket);
//			result.add(fillLastPacket);
//		}
//		return result;
//    }
    
   /**
     * Calculates the time cost of sending one evaluation.
     * Includes the time to copy tuples to the tray
     * Includes the time the radio uses to send the tuples but not the radio overhead.
     * 
     * Assumes that the correct location in the tray is done by a calculation rather than a search.
     * 
     * Based on the time estimates provided in the OperatorsMetaData file.
     * 
     * @return Time used by one evaluation.
 * @throws TypeMappingException 
 * @throws SchemaMetadataException 
 * @throws OptimizationException 
     */
    public final long getTimeCost(final DAF daf, final long bufferingFactor, 
    		CostParameters costParams) throws OptimizationException, SchemaMetadataException, TypeMappingException {
 		final int packets = packetsPerTask (daf, bufferingFactor, costParams);
		//CB: Copy time for producer moved to fragment.
		//CB: For relay and consumer I am assuming copy is done between receiving packets 
		//copyTime = tuples * OperatorMetaData.getCopyTuple();
		if (this.sourceSite.getID().equalsIgnoreCase(this.destSite.getID())) {
		    //CB:The work is done by the fragment Task
		    return 0;
		}
		//CB the TX and RX of a relay are plan seperately.
		return (long) Math.ceil(costParams.getSendPacket() * packets);
    }

//    /**
//     * Calculate the time to do thus Exchange.
//     * Ignores overhead such as turning radio on and off. 
//     * @param card CardinalityType The type of cardinality to be considered.
//     * @param daf Distributed query plan this operator is part of.
//	 * @param round Defines if rounding reserves should be included or not
//     * @return Expression for the time cost.
//     */
//    public final AlphaBetaExpression getTimeExpression(
//    		final CardinalityType card, final DAF daf, final boolean round) {
//    	if (isRemote) { 
//    		AlphaBetaExpression packets = this.packetsPerTask(card, daf, round);
//    		return AlphaBetaExpression.multiplyBy(
//    			packets, CostParameters.getSendPacket());
//    	} else {
//    		return new AlphaBetaExpression();
//    	}
//    }
//
//    public final ExchangePart clone(final DAF clonedDAF) {
//	final Fragment clonedSourceFrag = clonedDAF.getFragment(this.sourceFrag
//		.getID());
//	final Site clonedSourceSite = (Site) clonedDAF.getRoutingTree().getNode(
//		this.sourceSite.getID());
//	final Fragment clonedDestFrag = clonedDAF.getFragment(this.destFrag
//		.getID());
//	final Site clonedDestSite = (Site) clonedDAF.getRoutingTree().getNode(
//		this.destSite.getID());
//	final Site clonedCurrentSite = (Site) clonedDAF.getRoutingTree()
//		.getNode(this.currentSite.getID());
//	final ExchangePart clone = new ExchangePart(clonedSourceFrag,
//		clonedSourceSite, clonedDestFrag, clonedDestSite,
//		clonedCurrentSite, this.partType, this.isRemote, null); //TODO: clone the previous one
//	return clone;
//    }

    /**
     * Sets the destination fragment of this exchange part.
     * @param fragment
     */
	public void setDestFrag(InstanceFragment fragment) 
	{
		this.destFrag = fragment;
	}
	
	/**
	 * used to set up rewiring
	 * @param next
	 */
	public void setNextExchange(InstanceExchangePart next)
	{
	  this.next = next;
	}
	
	/**
	 * used in the overlay reconnection
	 * @return
	 */
  public String getPreviousId()
  {
    return previousId;
  }
	
	public void setPreviousExchange(InstanceExchangePart pre)
	{
	  this.prev = pre;
	}
	
	public void setSite(Site site)
	{
	  if(this.getSite() != null)
	  {
	    this.previousId = this.getSite().getID();
	    super.setSite(site);
	  }
	  else
	  {
	    super.setSite(site);
	  }
	}
	
	public void setDestinitionSite(Site newDestinationSite)
	{
	  this.destSite = newDestinationSite;
	  InstanceExchangePart nextPart = this.next;
	  while(nextPart != null && !nextPart.getDestSiteID().equals(newDestinationSite.getID()))
	  {
	    nextPart.setDestinitionSite(newDestinationSite);
	  }
	  InstanceExchangePart prePart = this.prev;
	  while(prePart != null && !prePart.getDestSiteID().equals(newDestinationSite.getID()))
    {
	    prePart.setDestinitionSite(newDestinationSite);
    }
	}
	
	public void setSourceFragment(InstanceFragment sourceFrag)
	{
	  this.sourceFrag = sourceFrag;
	  this.sourceSite = sourceFrag.getSite();
	  this.regenerateID();
	}
	
	public void setSourceSite(Site newSourceSite)
  {
    this.sourceSite = newSourceSite;
    InstanceExchangePart nextPart = this.next;
    while(nextPart != null && !nextPart.getSourceSiteID().equals(newSourceSite.getID()))
    {
      nextPart.setSourceSite(newSourceSite);
    }
    InstanceExchangePart prePart = this.prev;
    while(prePart != null && !prePart.getSourceSiteID().equals(newSourceSite.getID()))
    {
      prePart.setSourceSite(newSourceSite);
    }
  }

}

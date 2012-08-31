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
package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.util.HashSet;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

/**
 * This class represents a tasks which involves communication between two nodes in the query plan.
 * @author Ixent Galpin
 *
 */
public class CommunicationTask extends Task implements Comparable<CommunicationTask>{

	/**
   * serialVersionUID
   */
  private static final long serialVersionUID = -7823982575600894710L;

    //TODO move to task.
    //The site which is transmitting data
    private Site sourceNode = null;

    //The site which is receiving data
    private Site destNode = null;

    //The exchange components involved
    private HashSet<ExchangePart> exchangeComponents;

    //specifies whether task is sending or receiving
    private int mode;

    public static final int RECEIVE = 0;

    public static final int TRANSMIT = 1;
    
    public static final int ACKRECEIVE = 2;
    
    public static final int ACKTRANSMIT = 3;

   // private long alpha;

    private long beta;

    private HashSet<InstanceExchangePart> instanceExchangeComponents;
    private int maxPacketsEspectedToTransmit;
    
    public CommunicationTask(final long startTime, final Site sourceNode,
                             final Site destNode, final int mode, final long alpha, 
                             final long bufferingFactor, final DAF daf, int maxPackets,
                             CostParameters costParams) 
    throws OptimizationException, SchemaMetadataException, TypeMappingException 
    {
    super(startTime, costParams);
    this.sourceNode = sourceNode;
    this.destNode = destNode;
    this.instanceExchangeComponents = null;
    this.maxPacketsEspectedToTransmit = maxPackets;
    this.beta = bufferingFactor;
    this.endTime = startTime + (long) Math.ceil(costParams.getSendPacket() * maxPacketsEspectedToTransmit);
    this.mode = mode;
    
      }
    
    
    /**
     * Create an instance of a CommunicationTask.
     * @param startTime
     * @param sourceNode
     * @param destNode
     * @param tuplesToSend
     * @param mode
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     * @throws OptimizationException 
     */
    public CommunicationTask(final long startTime, final Site sourceNode,
	    final Site destNode,
	    final int mode, final HashSet<InstanceExchangePart> tuplesToSend,
	     final long alpha, final long bufferingFactor, final DAF daf,  
	    CostParameters costParams) throws OptimizationException, SchemaMetadataException, TypeMappingException {
	super(startTime, costParams);
	this.sourceNode = sourceNode;
	this.destNode = destNode;
	this.instanceExchangeComponents = tuplesToSend;
	this.beta = bufferingFactor;
	this.endTime = startTime + this.getTimeCost(daf);
	this.mode = mode;
	
    }
    
    public CommunicationTask(final long startTime, final Site sourceNode,
        final Site destNode, final int mode, Long packets, CostParameters costParams) 
    throws OptimizationException, SchemaMetadataException, TypeMappingException {
    super(startTime, costParams);
    this.sourceNode = sourceNode;
    this.destNode = destNode;
    this.endTime = startTime + this.getTimeCost(packets);
    this.mode = mode;
      }
    
    /**
     * Create an instance of a CommunicationTask.
     * @param startTime
     * @param sourceNode
     * @param destNode
     * @param tuplesToSend
     * @param mode
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     * @throws OptimizationException 
     */
    public CommunicationTask(final long startTime, final Site sourceNode,
      final Site destNode,
      final HashSet<ExchangePart> tuplesToSend,
      final int mode, final long alpha, final long bufferingFactor, final DAF daf,  
      CostParameters costParams) throws OptimizationException, SchemaMetadataException, TypeMappingException {
  super(startTime, costParams);
  this.sourceNode = sourceNode;
  this.destNode = destNode;
  this.exchangeComponents = tuplesToSend;
  this.beta = bufferingFactor;
  this.endTime = startTime + this.getTimeCost(daf);
  this.mode = mode;

    }

    /**
     * constructor which adds a defined overhead t the duration of the communication task 
     * (therefore keeping the radio turned on to encapsulate possible different sources in the 
     * unreliable channel strategy.)
     * @param startTime
     * @param sourceNode
     * @param destNode
     * @param mode
     * @param tuplesToSend
     * @param alpha
     * @param beta
     * @param daf
     * @param costParams
     * @param overhead
     * @throws OptimizationException
     * @throws SchemaMetadataException
     * @throws TypeMappingException
     */
    public CommunicationTask(long startTime, Site sourceNode, Site destNode,
        int mode, HashSet<InstanceExchangePart> tuplesToSend, long alpha,
        long beta, DAF daf, CostParameters costParams, Long overhead)
    throws OptimizationException, SchemaMetadataException, TypeMappingException
    {
      super(startTime, costParams);
      this.sourceNode = sourceNode;
      this.destNode = destNode;
      this.instanceExchangeComponents = tuplesToSend;
      this.beta = beta;
      this.endTime = startTime + this.getTimeCost(daf) + overhead;
      this.mode = mode;
    }


    public final Site getSourceNode() {
	return this.sourceNode;
    }

    public final String getSourceID() {
	return this.sourceNode.getID();
    }

    public final Site getDestNode() {
	return this.destNode;
    }

    public final String getDestID() {
	return this.destNode.getID();
    }

    public final int getMode() {
	return this.mode;
    }

  //  public final HashSet<ExchangePart> getExchangeComponents()
    //{
      //return exchangeComponents;
    //}
    
    public final HashSet<InstanceExchangePart> getInstanceExchangeComponents()
    {
      return instanceExchangeComponents;
    }
    
    public final CostParameters getCostParameters()
    {
      return this.costParams;
    }
    
    @Override
	public final Site getSite() 
  {
	  if (this.mode == RECEIVE) 
	    return this.destNode;
	  else if(this.mode == TRANSMIT)
	    return this.sourceNode;
	  else if(this.mode == ACKRECEIVE)
	    return this.destNode;
	  else
	    return this.sourceNode;
    }

    @Override
	public final String getSiteID() {
	return this.getSite().getID();
    }

    /**
     * Calculates the time overhead involved in a radio exchange.
     * 
     * Based on the time estimates provided in the OperatorsMetaData file.
     * 
     * @return time overhead of a radio message. 
     */
    public static long getTimeCostOverhead(CostParameters costParams) {
    	return (long) Math.ceil(costParams.getCallMethod()
    			+ costParams.getSignalEvent()
    			+ costParams.getTurnOnRadio()
    			+ costParams.getRadioSyncWindow() * 2
    			+ costParams.getTurnOffRadio());
    	}
        
//    /**
//     * Calculate the time to do thus Exchange.
//     * Includes overhead such as turning radio on and off. 
//     * @param card CardinalityType The type of cardinality to be considered.
//     * @param daf Distributed query plan this operator is part of.
//	 * @param round Defines if rounding reserves should be included or not
//     * @return Expression for the time cost.
//     */
//    private AlphaBetaExpression getTimeExpression(
//    		final CardinalityType card, final DAF daf, final boolean round) {
//    	AlphaBetaExpression result = new AlphaBetaExpression();
//    	final Iterator<ExchangePart> exchCompIter 
//			= this.exchangeComponents.iterator();
//    	while (exchCompIter.hasNext()) {
//    		result.add(exchCompIter.next().getTimeExpression(card, daf, round));
//    	}
//    	result.add(getTimeCostOverhead());
//    	return result;
//    }
//    
    /**
     * Returns the duration of this task.
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     * @throws OptimizationException 
     */
    @Override
	protected final long getTimeCost(final DAF daf) throws OptimizationException, SchemaMetadataException, TypeMappingException {
    	long result = 0;
	
    	if(instanceExchangeComponents == null)
    	{
      	final Iterator<ExchangePart> exchCompIter 
      		= this.exchangeComponents.iterator();
      	while (exchCompIter.hasNext()) {
      		final ExchangePart exchComp = exchCompIter.next();
      		if ((exchComp.getComponentType() 
      					== ExchangePartType.PRODUCER)
      				|| (exchComp.getComponentType() 
      					== ExchangePartType.RELAY)) {
      			result += exchComp.getTimeCost(daf, beta, costParams);
      			this.maxPacketsEspectedToTransmit += exchComp.getmaxPackets(daf, beta, costParams);
      		}
      	}
      	result += getTimeCostOverhead(costParams);
    	}
    	else
    	{
    	  final Iterator<InstanceExchangePart> exchCompIter 
        = this.instanceExchangeComponents.iterator();
      while (exchCompIter.hasNext()) {
        final InstanceExchangePart exchComp = exchCompIter.next();
        if ((exchComp.getComponentType() 
              == ExchangePartType.PRODUCER)
            || (exchComp.getComponentType() 
              == ExchangePartType.RELAY)) {
          result += exchComp.getTimeCost(daf, beta, costParams);
          this.maxPacketsEspectedToTransmit += exchComp.getmaxPackets(daf, beta, costParams);
        }
      }
      result += getTimeCostOverhead(costParams);
    	}
//IG: Might be best to have this check...
//    	assert (result <= getTimeExpression(
//    			CardinalityType.PHYSICAL_MAX, daf, true)
//    			.evaluate(alpha, this.bufferingFactor));
    	return result;
    	
    	/*
	 	BetaExpression expr = this.getTXTimeBetaExpression(qp);
	 	return (long)expr.evaluate(bufferingFactor);		
    	*/
    }

    
    /**
     * Returns the duration of this task (not daf related).
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     * @throws OptimizationException 
     */

  protected final long getTimeCost(final Long packets) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException 
  {
    long result = 0;
    result += (long) Math.ceil(costParams.getSendPacket() * packets);
    result += getTimeCostOverhead(costParams);
    return result;
  }
    

    /**
     * String representation.
     */
    @Override
	public final String toString() 
  {
	  if (this.mode == RECEIVE) 
	    return "RX " + this.sourceNode.getID() + "_" + this.destNode.getID();
	  else if(this.mode == TRANSMIT)
	    return "TX " + this.sourceNode.getID() + "_" + this.destNode.getID();
	  else if(this.mode == ACKRECEIVE)
	    return "RXA " + this.sourceNode.getID() + "_" + this.destNode.getID();
	  else 
	    return "TXA " + this.sourceNode.getID() + "_" + this.destNode.getID();
  }

  @Override
  public int compareTo(CommunicationTask other)
  {
    final int BEFORE = -1;
    final int EQUAL = 0;
    final int AFTER = 1;
    if(other.startTime < this.startTime)
      return BEFORE;
    else if(other.startTime == this.startTime)
      return EQUAL;
    else
      return AFTER;
  }
  
  public int getMaxPacektsTransmitted()
  {
    return this.maxPacketsEspectedToTransmit;
  }
}

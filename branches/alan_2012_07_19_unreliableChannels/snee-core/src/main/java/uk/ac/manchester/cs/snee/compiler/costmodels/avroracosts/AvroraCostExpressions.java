package uk.ac.manchester.cs.snee.compiler.costmodels.avroracosts;

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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePartType;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAcquireOperator;

/** 
 * This method holds the static cost models.
 * 
 * Most take a DAF and a Site and return AlphaBetaExpressions. 
 * @author Christian
 *
 */
public final class AvroraCostExpressions{
  
  /** Distributed Algebra Form for which these expressions are valid.*/ 
  private DAF daf;
  private CostParameters costParams;
  private AgendaIOT agenda;
  
  /** Format for display and showing decimal numbers. */
    private final DecimalFormat df = new DecimalFormat("0.000000");

  /** The full length of the agenda (alpha*beta). */
  private static final AlphaBetaExpression AGENDA_LENGTH = 
    new AlphaBetaExpression(1, 0, 0, 0);

  /** Constructor which stores the DAF. 
   * @param queryDaf Distribute Algebra Form
   */ 
  public AvroraCostExpressions(final DAF queryDaf, CostParameters costParams, AgendaIOT agenda) 
  {
    this.daf = queryDaf;
    this.costParams = costParams;
    this.agenda = agenda;
  }
  
  /** 
   * Generates an expression for the total energy cost 
   * for this site for this DAF for a single agenda evaluation.
   * Includes all fragments, and communication tasks.
   * 
   * @param site Site for which to generate costs.
   * @param round Defines if rounding reserves should be included or not
   * @return Energy cost function. 
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   */
  public AlphaBetaExpression getSiteEnergyExpression(final Site site,
      final boolean round, HashMap<String, AlphaBetaExpression> debug) 
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException 
  {
    
    AlphaBetaExpression energyCost = getCPUEnergyExpression(site, round, debug);
    AlphaBetaExpression sensorCost = getSensorEnergyExpression(site, round, debug);
    AlphaBetaExpression radioCost = getRadioEnergyExpression(site, round, debug);
    
    return AlphaBetaExpression.add(energyCost, 
        AlphaBetaExpression.add(sensorCost, radioCost));
  }
  
  public AlphaBetaExpression getSiteEnergyExpression(final Site site,
      final boolean round) 
  throws OptimizationException, SchemaMetadataException,
  TypeMappingException 
  {
    return getSiteEnergyExpression(site, round, null);
  }
  
  /** 
   * Generates an expression for the total CPU Energy 
   * consumption for this site on this DAF for a single agenda evaluation.
   * Includes all fragments, and communication tasks.
   * 
   * @param site Site for which to generate cost expressions.
   * @param round Defines if rounding reserves should be included or not
   * @return Energy cost function. 
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   */
  private AlphaBetaExpression getCPUEnergyExpression(
      final Site site, final boolean round, HashMap<String,AlphaBetaExpression> debug) 
  throws 
  OptimizationException, SchemaMetadataException, 
  TypeMappingException 
  {
    
    AlphaBetaExpression activeCPUTime = getCPUActiveDuration(site, round);
    AlphaBetaExpression activeCPUEnergy = AlphaBetaExpression.multiplyBy(
      activeCPUTime, 
      AvroraCostParameters.CPUACTIVEAMPERE 
      * AvroraCostParameters.VOLTAGE); // W = J/s = mJ/ms
    
    AlphaBetaExpression sleepCPUTime = getCPUPowerSaveDuration(site, round);
    AlphaBetaExpression sleepCPUEnergy = AlphaBetaExpression.multiplyBy(
      sleepCPUTime, 
      AvroraCostParameters.CPUPOWERSAVEAMPERE 
      * AvroraCostParameters.VOLTAGE);
    
    if (debug!=null) {
      debug.put("CPU active time (ms)", activeCPUTime);
      debug.put("CPU active energy (mJ)", activeCPUEnergy);
      debug.put("CPU sleep time (ms)", sleepCPUTime);
      debug.put("CPU sleep energy (mJ)", sleepCPUEnergy);
    }
      
    return AlphaBetaExpression.add(activeCPUEnergy, sleepCPUEnergy);
  }
  
  /** 
   * Generates an expression for the total Sensor energy 
   * for this site on this DAF for a single agenda evaluation.
   * Includes all fragments, and communication tasks.
   * 
   * @param site Site for which to generate costs.
   * @param round Defines if rounding reserves should be included or not
   * @return Energy cost function. 
   */
  private AlphaBetaExpression getSensorEnergyExpression(
      final Site site, final boolean round, HashMap<String,AlphaBetaExpression> debug) {
    
    AlphaBetaExpression result = new AlphaBetaExpression();
    Iterator<Fragment> fragments = site.getFragments().iterator();
    while (fragments.hasNext()) {
      Fragment fragment = fragments.next();
      if (fragment.containsOperatorType(SensornetAcquireOperator.class)) {
        result.addBetaTerm(0.00000003222*1000.0); //J to MilliJoules
      }
    }

    if (debug!=null) {
      debug.put("Sensor energy (mJ)", result);      
    }

    return result;
  }

  /**
   * Obtains the radio transmission level used at this site.
   * Information comes from the daf routing tree.
   * @param site Site to get tx level for.
   * @return Tx level for this site;
   */
  private int getTxLevel(final Site site) {
    if (site.getOutDegree() == 0) {
      return 0;
    }
    Site parent = (Site) site.getOutput(0);
    int txLevel = 
      (int) daf.getRT().getRadioLink(site, parent).getEnergyCost();
    return txLevel;
  }
  
  /** 
   * Generates an expression for the total Radio Energy 
   * for this site on this DAF for a single agenda evaluation.
   * Includes all fragments, and communication tasks.
   * 
   * @param site Site for which to generate costs.
   * @param round Defines if rounding reserves should be included or not
   * @return Energy cost function. 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  private AlphaBetaExpression getRadioEnergyExpression(
      final Site site, final boolean round, HashMap<String,AlphaBetaExpression> debug) 
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException 
  {
    
    int txLevel = getTxLevel(site);
    double txPower =  AvroraCostParameters.RADIOTRANSMITAMPERE[txLevel]
        * AvroraCostParameters.VOLTAGE;
    double rxPower = AvroraCostParameters.RADIORECEIVEAMPERE 
    * AvroraCostParameters.VOLTAGE;
    
    AlphaBetaExpression txTime = getRadioTransmitDuration(site, round);
    AlphaBetaExpression txEnergy = AlphaBetaExpression.multiplyBy(txTime,txPower);
    AlphaBetaExpression rxTime = getRadioReceiveDuration(site, round);
    AlphaBetaExpression rxEnergy = AlphaBetaExpression.multiplyBy(rxTime, rxPower);

    if (debug!=null) {
      debug.put("TX time (ms)", txTime);
      debug.put("TX energy (mJ)", txEnergy);
      debug.put("RX time (ms)", rxTime);
      debug.put("RX energy (mJ)", rxEnergy);    
    }
      
    return AlphaBetaExpression.add(txEnergy, rxEnergy);
  }
  
  public double getPacketEnergyExpression(
      final Site source, final Site dest, final boolean round, HashMap<String,AlphaBetaExpression> debug) 
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException 
  {
    int txLevel = (int) daf.getRT().getRadioLink(source, dest).getEnergyCost();
    double txPower =  AvroraCostParameters.RADIOTRANSMITAMPERE[txLevel] * AvroraCostParameters.VOLTAGE;
    double rxPower = AvroraCostParameters.RADIORECEIVEAMPERE * AvroraCostParameters.VOLTAGE;
    return txPower + rxPower;
  }
  /**
   * Generates an expression for the total Energy used in writing to flash
   * 
   */
  public AlphaBetaExpression getFlashEnergyExpression(final int bytes, 
                              HashMap<String,AlphaBetaExpression> debug) 
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException 
  {
    
    AlphaBetaExpression writeCost =  new AlphaBetaExpression(AvroraCostParameters.FlashWRITEAMPERE
        * AvroraCostParameters.VOLTAGE);
    
    AlphaBetaExpression FlashCost = getRadioTransmitDuration(writeCost, bytes);

    if (debug!=null) {
      debug.put("Flash Energy (mJ)", FlashCost);  
    }
      
    return FlashCost;
  }
  
  private AlphaBetaExpression getRadioTransmitDuration(AlphaBetaExpression writeCost, int bytes)
  {
    return AlphaBetaExpression.multiplyBy(writeCost, bytes);
  }

  /** 
   * Generates an expression for the total time 
   * for all fragments 
   * for this site on this DAF for a single agenda evaluation.
   * 
   * @param site Site for which to generate costs.
     * @param card CardinalityType The type of cardinality to be considered.
   * @param round Defines if rounding reserves should be included or not
   * @return Energy cost function. 
   * @throws OptimizationException 
   */
  private AlphaBetaExpression getFragmentsDuration(
      final CardinalityType card, final Site site, final boolean round) throws OptimizationException {
    AlphaBetaExpression result = new AlphaBetaExpression();
    Iterator<Fragment> fragments = site.getFragments().iterator();
    while (fragments.hasNext()) {
      Fragment fragment = fragments.next();
      result.add(new AlphaBetaExpression(fragment.getTimeCost(site, daf, costParams)));
    }
    return result;
  }


  /** 
   * Generates an expression for the total time 
   * taken for all exchanges,  
   * for this site on this DAF for a single agenda evaluation.
   * 
   * Includes overheads such as turning radio on/offs.
   * 
   * @param site Site for which to generate costs.
     * @param card CardinalityType The type of cardinality to be considered.
   * @param round Defines if rounding reserves should be included or not
   * @return Energy cost function. 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  private AlphaBetaExpression getSiteCommunicationDuration(
      final CardinalityType card, final Site site, final boolean round) 
  throws 
  OptimizationException, SchemaMetadataException, 
  TypeMappingException 
  {
    AlphaBetaExpression result = new AlphaBetaExpression();
    //Count sites involved in comms.
    HashSet<String> partners = new HashSet<String>();
    //Add duration of each exchange.
    Iterator<ExchangePart> exchangeComponents = 
      site.getExchangeComponents().iterator();
    while (exchangeComponents.hasNext()) {
      final ExchangePart exchangeComponent = 
        exchangeComponents.next();
      result.add(
          exchangeComponent.getTimeCost(daf, agenda.getBufferingFactor(), costParams));
      //Relays have to be added twice 
      //once for the rx and again for the tx.
      if (exchangeComponent.getComponentType() 
          == ExchangePartType.RELAY) {
        result.add(
            exchangeComponent.getTimeCost(daf, agenda.getBufferingFactor(), costParams));
      }
      //Add sites involved.
      partners.add(exchangeComponent.getSourceSiteID());
      partners.add(exchangeComponent.getDestSiteID());
    }
      //remove one getBetweenpackets cost for first packet
    result.subtract(AvroraCostParameters.getBetweenPackets());
    //Assume that all exchanges with each partner 
    //will be grouped into single task.
    //Add overhead for all source/ destinations found except self.
    result.add(CommunicationTask.getTimeCostOverhead(costParams, false, true) * (partners.size() - 1)); 
                                 //does not include radio on/off
    result.add((partners.size() - 1) * 
        costParams.getTurnOnRadio());
    return result;
  }

  /** 
   * Generates an expression for the total time spent in CPUActive mode 
   * for this site on this DAF for a single agenda evaluation.
   * Includes all fragments, and communication tasks.
   * 
   * @param site Site for which to generate costs.
   * @param round Defines if rounding reserves should be included or not
   * @return Energy cost function. 
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   */
  private AlphaBetaExpression getCPUActiveDuration(final Site site, 
      final boolean round) 
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException 
  {
    AlphaBetaExpression result = getFragmentsDuration(
        CardinalityType.MAX, site, round);
    result.add(getSiteCommunicationDuration(CardinalityType.MAX, site, round));
    return result;
  }
    
  /*
  private AlphaBetaExpression getTransmitDuration(final Site site, 
      final boolean round) {
    AlphaBetaExpression expression = new AlphaBetaExpression();
    Iterator<ExchangeComponent> exchangeComponents = 
      site.getExchangeComponents().iterator();
    while (exchangeComponents.hasNext()) {
      final ExchangeComponent exchangeComponent = 
        exchangeComponents.next();
        if ((exchangeComponent.getComponentType() 
              == ExchangeComponent.EXCHANGE_PRODUCER)
            || (exchangeComponent.getComponentType() 
              == ExchangeComponent.EXCHANGE_RELAY)) {
          //TODO determine which is more accurate Packets or bytes.
          AlphaBetaExpression packets = exchangeComponent.packetsPerTask(
              CardinalityType.MAX, daf, true);
          expression.add(packets);
        }
    }
    return expression;
  }

  
  /** 
   * Generates an expression for the total time spent 
   * to do the exchanges in an agenda evaluation. 
   * 
   * @param round Defines if rounding reserves should be included or not
   * @return Time cost function. 
   * /
  private AlphaBetaExpression getTransmitDurationForAllSites(final boolean round) {
    AlphaBetaExpression expression = 
      new AlphaBetaExpression(0, 0, 0, 0);

    //Assumes each TX and relay is only on one site.
    //Assumes no two exchanges are timed together.
    //Radio is on on all Sites during all transmissions.
    Iterator<Site> sites = daf.siteIterator(TraversalOrder.POST_ORDER);
    while (sites.hasNext()) {
      expression.add(getTransmitDuration(sites.next()));
    }
    return expression;
  }
  
  /** 
   * Generates an expression for the total time spent 
   * in CPU Active or Idle mode 
   * for this site on this DAF.
   * Includes all fragments, and communication tasks.
   * 
   * @param site Site for which to generate costs.
   * @param round Defines if rounding reserves should be included or not
   * @return Time cost function. 
   */
  private AlphaBetaExpression getCPUActiveOrIdleDuration(final Site site, final boolean round) 
  throws 
  OptimizationException, SchemaMetadataException, 
  TypeMappingException 
  {
    //Assume radio of so CPU can sleep once average done.
    AlphaBetaExpression result = getFragmentsDuration(
        CardinalityType.AVERAGE, site, round);
    //Radio can not sleep so 
    result.add(getSiteCommunicationDuration(CardinalityType.MAX, site, round));
    return result;
  }
  
  /** 
   * Generates an expression for the total time spent 
   * to do the last Epoch in an agenda evaluation, 
   * including idle time.
   * Includes all fragments, and communication tasks.
   * 
   * /
  private void computeLastEpochDuration() {
    lastEpochDuration = AlphaBetaExpression.add(
        getLastEpochFragmentsDuration(true), 
        getTransmitDurationForAllSites(true));
  }
  
  /** 
   * Generates an expression for the total time spent in CPU Idle mode 
   * for this site on this DAF during a single agenda evaluation.
   * Includes all fragments, and communication tasks.
   * 
   * @param site Site for which to generate costs.
   * @param round Defines if rounding reserves should be included or not
   * @return Time cost function. 
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   */
  private AlphaBetaExpression getCPUIdleDuration(final Site site, final boolean round) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException 
  {
    return AlphaBetaExpression.subtract(getCPUActiveOrIdleDuration(site, round),
                                        getCPUActiveDuration(site, round));
  }
  
  /** 
   * Generates an expression for the total time 
   * spent in CPU "Power Save" mode 
   * for this site on this DAF during a single agenda evaluation.
   * Includes all fragments, and communication tasks.
   * 
   * @param site Site for which to generate costs.
   * @param round Defines if rounding reserves should be included or not
   * @return Energy cost function. 
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   */
  private AlphaBetaExpression getCPUPowerSaveDuration(final Site site, final boolean round) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException {
    return AlphaBetaExpression.subtract(AGENDA_LENGTH, getCPUActiveOrIdleDuration(site, round));
  }

  /** 
   * Generates an expression for the packets sent  
   * for given collection of exchange components.
   * 
   * @param site Site for which to generate costs.
   * @param round Defines if rounding reserves should be included or not
   * @return Packets sent expression 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  public AlphaBetaExpression getPacketsSent(HashSet<InstanceExchangePart> exchComps, 
      final boolean round) 
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException 
  { 
    AlphaBetaExpression expression = new AlphaBetaExpression();
    Iterator<InstanceExchangePart> exchCompIter = 
      exchComps.iterator();
    while (exchCompIter.hasNext()) {
      final InstanceExchangePart exchangeComponent = 
        exchCompIter.next();
        if ((exchangeComponent.getComponentType() 
              == ExchangePartType.PRODUCER)
            || (exchangeComponent.getComponentType() 
              == ExchangePartType.RELAY)) {
          //TODO determine which is more accurate Packets or bytes.
          AlphaBetaExpression packets = new AlphaBetaExpression(
            exchangeComponent.packetsPerTask(daf, agenda.getBufferingFactor(), costParams));
            //.packetsPerTask(
            //  CardinalityType.MAX, daf, round);
          expression.add(packets);
        }
    }
    return expression;
  }
  
  public AlphaBetaExpression getPacketsSent(double packetsSent, final boolean round) 
  {
    AlphaBetaExpression expression = new AlphaBetaExpression();
    AlphaBetaExpression packets = new AlphaBetaExpression(packetsSent);
    expression.add(packets);
    return expression;
  }
  
  
  /** 
   * Generates an expression for the total time spent in Radio Transmit (Tx) 
   * for this site on this DAF.
   * Includes all communication tasks (although in a single-query evaluation
   * context, there is only one CommunicationTask).
   * Result is for any TX level.
   * 
   * @param site Site for which to generate costs.
   * @param round Defines if rounding reserves should be included or not
   * @return Time cost function. 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  private AlphaBetaExpression getRadioTransmitDuration(final Site site, 
      final boolean round) 
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException 
  {
    HashSet<InstanceExchangePart> exchComps = site.getInstanceExchangeComponents(); 
    return AlphaBetaExpression.multiplyBy(this.getPacketsSent(exchComps, round),
        AvroraCostParameters.PACKETTRANSMIT);
  }

  /** 
   * Generates an expression for the total time spent 
   * in Radio Receive (Rx) and Transmit (Tx) mode 
   * for this site on this DAF.
   * Includes all fragments, and communication tasks.
   * 
   * @param site Site for which to generate costs.
   * @param round Defines if rounding reserves should be included or not
   * @return Time cost function. 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  private AlphaBetaExpression getRadioOnDuration(final Site site, 
      final boolean round) 
  throws 
  OptimizationException, SchemaMetadataException, 
  TypeMappingException 
  {
    return getSiteCommunicationDuration(CardinalityType.MAX, site, round);
  }

  
  /** 
   * Generates an expression for the total time spent 
   * in Radio Receive (Rx) mode 
   * for this site on this DAF.
   * Includes all fragments, and communication tasks.
   * 
   * @param site Site for which to generate costs.
   * @param round Defines if rounding reserves should be included or not
   * @return Time cost function. 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  private AlphaBetaExpression getRadioReceiveDuration(final Site site, 
      final boolean round) 
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException 
  {
    return AlphaBetaExpression.subtract(getRadioOnDuration(site, round),
        getRadioTransmitDuration(site, round));
  }

  /** 
   * Generates an expression for the total time spent 
   * in Radio Power Down Mode 
   * for this site on this DAF.
   * 
   * @param site Site for which to generate costs.
   * @param round Defines if rounding reserves should be included or not
   * @return Time cost function. 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  private AlphaBetaExpression getRadioPowerDownDuration(final Site site, 
      final boolean round) 
  throws 
  OptimizationException, SchemaMetadataException, 
  TypeMappingException 
  {
    return AlphaBetaExpression.subtract(AGENDA_LENGTH,
        getRadioOnDuration(site, round));
  }

  /** 
   * Generates an expression for the total time spent in Sensor Active mode 
   * for this site on this DAF.
   *  
   * @param site Site for which to generate costs.
   * @param round Defines if rounding reserves should be included or not. 
   *    Has not affect but added to keep signatures equal.
   * @return Time cost function. 
   */
  private AlphaBetaExpression getSensorActiveDuration(final Site site, 
    final boolean round) {
    return AGENDA_LENGTH;
  }

  /** 
   * Generates an expression for the total time spent in Sensor Active mode 
   * for this site on this DAF.
   *  
   * @param site Site for which to generate costs.
   * @param round Defines if rounding reserves should be included or not
   * @return Time cost function. 
   */
  private AlphaBetaExpression getSensorSleepDuration(final Site site, 
      final boolean round) {
    AlphaBetaExpression result = new AlphaBetaExpression();
    return result;
  }
  
  /** 
   * Generates an expression for the total memory 
   * for this site on this DAF.
   * Includes all fragments, and communication tasks.
   * 
   * @param site Site for which to generate costs.
   * @param round Defines if rounding reserves should be included or not
   * @return Time cost function. 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  public AlphaBetaExpression getSiteMemoryExpression(final Site site, 
      final boolean round) 
  throws 
  OptimizationException, SchemaMetadataException,
  TypeMappingException 
  {
    double fragmentMemory = 0;
    Iterator<Fragment> fragments = site.getFragments().iterator();
    while (fragments.hasNext()) {
      Fragment fragment = fragments.next();
      //TODO Add overhead to memory
      fragmentMemory += fragment.getDataMemoryCost(site, daf);
    }
    AlphaBetaExpression expression = 
      new AlphaBetaExpression(0, 0, fragmentMemory, 0);
    Iterator<ExchangePart> exchangeComponents = 
      site.getExchangeComponents().iterator();
    while (exchangeComponents.hasNext()) {
      final ExchangePart exchangeComponent = 
        exchangeComponents.next();
        if ((exchangeComponent.getComponentType() 
              == ExchangePartType.PRODUCER)
            || (exchangeComponent.getComponentType() 
              == ExchangePartType.RELAY)) {
          expression.add(
              exchangeComponent.getDataMemoryCost(site, daf));
        }
    }
    return expression;
  }
  
  /** 
   * Displays the cost expression for debugging and reporting.
   * @throws SNEEConfigurationException 
   */
  public void display() 
  throws SNEEConfigurationException 
  {
    String latexFilename = SNEEProperties.getSetting(
        SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR) 
      + daf.getQueryName() + "-cost-expressions.tex";
    exportToLatex(latexFilename);
  }

  /** 
   * Displays how a duration is converted into energy.
   * @param out File to write to.
   * @param desc Description of type of duration
   * @param duration Time cost
   * @param ampere Ampere used by this state
   */
  private void displayBreakDown(final PrintWriter out, final String desc,
      final AlphaBetaExpression duration,   final double ampere) {
      out.print(desc + ": &"); 
    out.println("$" + duration.toLatexString() + "$\\\\");
    out.println("Ampere: & " + df.format(ampere)  +  " ampere \\\\");
    out.println("Voltage: &" + df.format(AvroraCostParameters.VOLTAGE) 
        + " volts \\\\");
    double energy = ampere * AvroraCostParameters.VOLTAGE;
    out.println("Energy Factor&" + df.format(energy) + " watts \\\\");
    AlphaBetaExpression cost 
      = AlphaBetaExpression.multiplyBy(duration, energy);
    out.println("Energy cost &$" + cost.toDecimalLatexString() 
        + " joules $\\\\");
    out.println("\\hline");
  }
  
  /** Exports an overview of the Cost Expressions used to Latex. 
   * @param fname Full Name of the file to write the latex to. 
   */
    public void exportToLatex(final String fname) {
      try {
        String beginTable = "\\begin{tabular}{p{5cm}p{5cm}}";
        final PrintWriter out = new PrintWriter(new BufferedWriter(
            new FileWriter(fname)));
        out.println("\\documentclass[a4paper]{article}");
        out.println("\\begin{document}");
        out.println("Daf = " + daf.getQueryName() + "\n");
          
          out.println("Total Enery\n");
        Iterator<Site> sites = daf.getRT().siteIterator(TraversalOrder.POST_ORDER);
          out.println(beginTable);
          out.println("\\hline");
        while (sites.hasNext()) {
          Site site = sites.next();
              out.print("Total Energy for Site "+site.getID()+": &"); 
            out.println("$" + this.getSiteEnergyExpression(site, true)
                .toDecimalLatexString() + "$ joules\\\\");
        }
            out.println("\\hline");
          out.println("\\end{tabular}\n");   
              
          out.println("Total Memory \n");
        sites = daf.getRT().siteIterator(TraversalOrder.POST_ORDER);
          out.println(beginTable);
          out.println("\\hline");
        while (sites.hasNext()) {
          Site site = sites.next();
              out.print("Total memory for Site "+site.getID()+": &"); 
            out.println("$" + this.getSiteMemoryExpression(site, true)
                .toDecimalLatexString() + "$ bytes\\\\");
        }
            out.println("\\hline");
          out.println("\\end{tabular}\n");   

            sites = daf.getRT().siteIterator(TraversalOrder.POST_ORDER);
        while (sites.hasNext()) {
          Site site = sites.next();
            out.println("\\newpage Costs for Site: " + site.getID() + "\n");
            
            out.println("CPU\n");
            out.println(beginTable);
            out.println("\\hline");
              out.println("\\hline");
              displayBreakDown(out, "Active Duration", 
              this.getCPUActiveDuration(site, true),
                AvroraCostParameters.CPUACTIVEAMPERE);

            out.print("Active or Idle Duration: &$"); 
            out.println(this.getCPUActiveOrIdleDuration(site, true)
                .toLatexString() + "$\\\\");
            out.println("\\hline");
              
              displayBreakDown(out, "Idle Duration", 
                  this.getCPUIdleDuration(site, true),
                    AvroraCostParameters.CPUIDLEAMPERE); 

              displayBreakDown(out, "Power Save Duration", 
                  this.getCPUPowerSaveDuration(site, true),
                    AvroraCostParameters.CPUPOWERSAVEAMPERE); 

              out.print("CPU Energy Expression: &"); 
            out.println("$" + this.getCPUEnergyExpression(site, true, null)
                .toDecimalLatexString() + "$ joules\\\\");
            out.println("\\hline");
            out.println("\\hline");
              out.println("\\end{tabular}\n");                

              out.println("Radio\n");
            out.println(beginTable);
              out.println("\\hline");
              out.print("Packets Sent: &$");
              HashSet<InstanceExchangePart> exchComps = site.getInstanceExchangeComponents();
            out.println(this.getPacketsSent(exchComps, true)
                .toLatexString() + "$\\\\");
                out.println("\\hline");
            
              out.print("Transmission strength: &"); 
            int txLevel = getTxLevel(site);
            out.println(txLevel + "\\\\");
                out.println("\\hline");
                
              displayBreakDown(out, "Transmit Duration", 
                  this.getRadioTransmitDuration(site, true),
                    AvroraCostParameters.getTXAmpere(txLevel));

            out.print("Radio On Duration: &$"); 
            out.println(this.getRadioOnDuration(site, true)
                .toLatexString() + "$\\\\");

              displayBreakDown(out, "Receive Save Duration", 
                  this.getRadioReceiveDuration(site, true),
                    AvroraCostParameters.RADIORECEIVEAMPERE);

              displayBreakDown(out, "Power down Duration", 
                  this.getRadioPowerDownDuration(site, true),
                    AvroraCostParameters.RADIOPOWERDOWNAMPERE);

              out.print("Radio Energy Expression: &"); 
            out.println("$" + this.getRadioEnergyExpression(site, true, null)
                .toDecimalLatexString() + "$ joules\\\\");
            out.println("\\hline");
            out.println("\\hline");
              out.println("\\end{tabular}\n");                

              out.println("Sensor\n");
            out.println(beginTable);
            
              displayBreakDown(out, "Active Duration", 
                  this.getSensorActiveDuration(site, true),
                    AvroraCostParameters.SENSORONAMPERE);
  
              out.print("Sensor sleep Duration: &$"); 
            out.println(this.getSensorSleepDuration(site, true)
                .toLatexString() + "$\\\\");
              out.println("\\hline");

              out.print("Sensor Energy Expression: &"); 
            out.println("$" + this.getSensorEnergyExpression(site, true, null)
                .toDecimalLatexString() + "$ joules\\\\");       
              out.println("\\hline");           
            out.println("\\end{tabular}\n");   
              
            out.println("Total\n");
              out.println(beginTable);
              out.println("\\hline");           
              out.print("Total Energy Expression: &"); 
            out.println("$" + this.getSiteEnergyExpression(site, true)
                .toDecimalLatexString() + "$ joules\\\\");       
              out.println("\\hline");           
              out.println("\\hline");           
            out.println("\\end{tabular}\n");   
              
              out.println(beginTable);
              out.println("\\hline");
              out.println("memory &$");
              out.println(getSiteMemoryExpression(site, true)
                .toLatexString() + "$\\\\");
              out.println("\\hline");
              out.println("\\end{tabular}\n");                
        }
        out.println("\ntest successful.");
        out.println("\\end{document}");
        out.close();
      } catch (final Exception e) 
      {
      }
    }
}

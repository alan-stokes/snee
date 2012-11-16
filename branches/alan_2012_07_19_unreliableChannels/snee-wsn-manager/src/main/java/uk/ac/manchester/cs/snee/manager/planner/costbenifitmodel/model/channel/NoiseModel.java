package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel;

//class ported from Hyungjune lee from stanford. 

import java.io.IOException;
import java.util.Random;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.RadioLink;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

public class NoiseModel
{
  //default global values.
  private boolean useingClearChannels;
  private boolean relibaleChannels;
  private boolean veriablePower;
  private Double transmissionPower; 
  private CostParameters costs;
  private Double signalFrequencyInMegaHerze;
  private Topology network;
  private CPMModel cpmModel;

  //values used within the RSSI forumla
  private static double pathLossExponent;

  
  
  public NoiseModel(Topology network, CostParameters costs) 
  throws SNEEConfigurationException, IOException
  {
    this.costs = costs;
    this.network = network;
    useingClearChannels = 
      SNEEProperties.getBoolSetting(SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_CLEANRADIO);
    relibaleChannels = 
       SNEEProperties.getBoolSetting(SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS);
    veriablePower = 
      SNEEProperties.getBoolSetting(SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_VERIABLEPOWER);
    signalFrequencyInMegaHerze =
      Double.parseDouble(SNEEProperties.getSetting(
          SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_SIGNALFREQUENCY));
    pathLossExponent = 
      Double.parseDouble(SNEEProperties.getSetting(
          SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_PATHLOSSEXPONENT));
    if(!veriablePower)
      transmissionPower = 
        Double.parseDouble(SNEEProperties.getSetting(
            SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_TRANSMISSIONPOWER));
    else
    {
      //TODO impliment some sort of arbitary transmission power. 
    }
    
    cpmModel = new CPMModel(network);
  }

  /**
   * entrance to the noise model for channel model.
   * Uses the noise model to determine if a packet has been recieved by 
   * DestID from sourceID though their direct link
   * @param sourceID
   * @param DestID
   * @return
   */
  public boolean packetRecieved(String sourceID, String destID, Long startTime)
  {
    if(useingClearChannels && relibaleChannels)
    {
      return true;
    }
    else
      return didPacketGetRecieved(sourceID, destID, startTime);
  }

  /**
   * helper method for the entrance method. 
   * @param sourceID
   * @param destID
   * @return
   */
  private boolean didPacketGetRecieved(String sourceID, String destID, Long startTime)
  {
    
    /**
     * PRR is caulcated by
     * PRR = (1 - (1/2 * exp - (S.N / 2)(1/0.64))) ^ 8 * packetsize
     * from Analyzing the transitional region in low power wireless links Marco Zuniga and Bhaskar Krishbaneachari
     */
    int packetSize = costs.getMaxMessagePayloadSize() + costs.getPayloadOverhead();
    Integer noise = cpmModel.getNoise(sourceID, startTime);
    Double RSSI = this.getRSSI(sourceID, destID);
    Double signalNoiseRatio = RSSI / noise;
    Double prr = (1 - (0.5 * Math.pow(Math.exp(0 - (signalNoiseRatio / 2) * (1/0.64)), 8 * packetSize)));
    
    Random random = new Random(new Long(0));
    double randomDouble = random.nextDouble();
    if(randomDouble > (1-prr))
    {
      return true;
    }
    else
    {
      return false;
    }
  }

 

  /**
   * returns the estimated signal strnegth though RSSI given 2 node ids.
   * @param sourceID
   * @param destID
   * @return
   */
  private Double getRSSI(String sourceID, String destID)
  {
    RadioLink link = 
      network.getRadioLink(network.getSite(sourceID), network.getSite(destID));
    Double distanceBetweenNodes = link.getDistanceCost();
    if(!this.veriablePower)
    {
      /**rssi value is determined by RSSI = (PL(D0) + 10 n log (d / d0)
      // d = distance between transmitter and reciever.
       * d0 the reference distance (telosb mote 75m)
       * n path loss exponent (free space = 2, urban area cellular raido 2.7 to 3.5, 
       *                       shaowed urban celluar radio 3 to 5,
       *                       in building line of sight 1.6 to 1.8
       *                       obstructed in building 4 to 6
       *                       obstructed in factories 2 to 3)
      */
      
      /**
       * Free space path loss forumla is used to detemrine PL(d0) 
       * PL(d0) (dB) = 10 log 10 ((((4 * pi ) / light ) * d0 * f) ^2)
       * From wikipedia 
       */
      Double PL_Do_ =  Math.log10(Math.pow((((4 * Math.PI) / NoiseModelConstants.C_LIGHT) * NoiseModelConstants.d0 * signalFrequencyInMegaHerze), 2));
      
      //quassian random variable for path loss forumla.
      Random random = new Random(new Long(0));
      Double quassianRandomVariable = random.nextGaussian();
      quassianRandomVariable = quassianRandomVariable * NoiseModelConstants.RSSI_DISTANCE_DEPENDENT_MEAN;
      /**
       * PL(d) (dB) = PL_d0_ + 10 * n * log(d/d0) + X 
       * From Analyzing the low power wireless links for WSN (md. mainul islam mamun, tarek hasan-al-mahmud,
       *      Typhoon: A reliable data dissemination protocol for wsn (chieh-jan mike liang, Razvan Musaloiu-E
       *      Wireless Communications principles and practice second edition (Theodore S. Rappaport)
       */                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
       
      Double pathLoss = PL_Do_ + (10 * pathLossExponent * Math.log((distanceBetweenNodes / NoiseModelConstants.d0))) + quassianRandomVariable;
      /** RSSI is measured in dB **/
      Double RSSI = transmissionPower - pathLoss;
      return RSSI;  
    }
    else
    {
      //TODO determine correct arbiarty power to make RSSI
      return null;
    }
  }
}

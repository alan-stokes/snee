package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;

public class NoiseModel
{
  private boolean useingClearChannels;
  private boolean relibaleChannels;
  
  public NoiseModel() throws SNEEConfigurationException
  {
    useingClearChannels = 
      SNEEProperties.getBoolSetting(SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_CLEANRADIO);
    relibaleChannels = 
       SNEEProperties.getBoolSetting(SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS);
  }
  
  public boolean packetRecieved(String sourceID, String DestID)
  {
    if(useingClearChannels && relibaleChannels)
    {
      return true;
    }
    else
      return didPacketGetRecieved(sourceID, DestID);
  }

  private boolean didPacketGetRecieved(String sourceID, String destID)
  {
    // TODO Auto-generated method stub
    return false;
  }
}

package uk.ac.manchester.cs.snee.sncb;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;

public class TinyOS_SNCB_Controller implements SNCB 
{
  private Logger logger = Logger.getLogger(TinyOS_SNCB_Controller.class.getName());
  private SNCB sncb;
  
  
  public TinyOS_SNCB_Controller() throws SNCBException
  {
    if (logger.isDebugEnabled())
      logger.debug("ENTER TinyOS_SNCB()");
    try 
    {
      //get target of compiler, decide on which version of SNCB to run
      CodeGenTarget target = CodeGenTarget.parseCodeTarget(SNEEProperties
          .getSetting(SNEEPropertyNames.SNCB_CODE_GENERATION_TARGET));
      if(target == CodeGenTarget.TELOSB_T2)
      {
        sncb = new TinyOS_SNCB_TelosB();
      }
      else if(target == CodeGenTarget.AVRORA_MICA2_T2 || target == CodeGenTarget.AVRORA_MICAZ_T2)
      {
        sncb = new TinyOS_SNCB_Avrora();
      }
      else if(target == CodeGenTarget.TOSSIM_T2)
      {
        sncb = new TinyOS_SNCB_Tossim();
      }
      else
      {
        
      }
        
        
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }
  }
  
  @Override
  public void init(String topFile, String resFile) throws SNCBException
  {
    sncb.init(topFile, resFile); 
  }
  
  @Override
  public SNCBSerialPortReceiver register(SensorNetworkQueryPlan qep,
      String queryOutputDir, CostParameters costParams) throws SNCBException
  {
    return sncb.register(qep, queryOutputDir, costParams);
  }
  
  @Override
  public void deregister(SensorNetworkQueryPlan qep) throws SNCBException
  {
    sncb.deregister(qep);
  }
  
  @Override
  public void start() throws SNCBException
  {
    sncb.start();
  }
  
  @Override
  public void stop(SensorNetworkQueryPlan qep) throws SNCBException
  {
    sncb.stop(qep);
  }
}
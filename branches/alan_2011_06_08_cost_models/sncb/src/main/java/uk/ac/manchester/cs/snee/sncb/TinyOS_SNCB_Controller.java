package uk.ac.manchester.cs.snee.sncb;

import java.io.IOException;

import net.sf.cglib.core.CodeGenerationException;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;

public class TinyOS_SNCB_Controller implements SNCB 
{
  private Logger logger = Logger.getLogger(TinyOS_SNCB_Controller.class.getName());
  private SNCB sncb;
  
  
  public TinyOS_SNCB_Controller(double duration) throws SNCBException
  {
    if (logger.isDebugEnabled())
      logger.debug("ENTER TinyOS_SNCB()");
    try 
    {
      CodeGenTarget target = CodeGenTarget.TELOSB_T2;
      //get target of compiler, decide on which version of SNCB to run
      if (SNEEProperties.isSet(SNEEPropertyNames.SNCB_CODE_GENERATION_TARGET)) 
      {
       target = CodeGenTarget.parseCodeTarget(SNEEProperties
          .getSetting(SNEEPropertyNames.SNCB_CODE_GENERATION_TARGET));
      }
      if(target == CodeGenTarget.TELOSB_T2)
      {
        sncb = new TinyOS_SNCB_TelosB(duration);
      }
      else if(target == CodeGenTarget.AVRORA_MICA2_T2 || target == CodeGenTarget.AVRORA_MICAZ_T2)
      {
        sncb = new TinyOS_SNCB_Avrora(duration);
      }
      else if(target == CodeGenTarget.TOSSIM_T2)
      {
        sncb = new TinyOS_SNCB_Tossim(duration);
      }
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }
  }
  
  public TinyOS_SNCB_Controller()
  {
    if (logger.isDebugEnabled())
      logger.debug("ENTER TinyOS_SNCB()");
    try 
    {
      CodeGenTarget target = CodeGenTarget.TELOSB_T2;
      //get target of compiler, decide on which version of SNCB to run
      if (SNEEProperties.isSet(SNEEPropertyNames.SNCB_CODE_GENERATION_TARGET)) 
      {
       target = CodeGenTarget.parseCodeTarget(SNEEProperties
          .getSetting(SNEEPropertyNames.SNCB_CODE_GENERATION_TARGET));
      }
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

  @Override
  public void waitForQueryEnd() throws InterruptedException
  {
    sncb.waitForQueryEnd();  
  }

  @Override
  public SNCBSerialPortReceiver register(SensorNetworkQueryPlan qep,
      String queryOutputDir, MetadataManager metadata) throws SNCBException
  {
    return sncb.register(qep, queryOutputDir, metadata);
  }

  @Override
  public void setOutputFolder(String newTargetDir)
  {
    sncb.setOutputFolder(newTargetDir);
  }

  @Override
  public void generateNesCCode(SensorNetworkQueryPlan qep,
      String queryOutputDir, MetadataManager metadata) throws IOException,
      SchemaMetadataException, TypeMappingException, OptimizationException,
      uk.ac.manchester.cs.snee.sncb.CodeGenerationException
  {
    sncb.generateNesCCode(qep, queryOutputDir, metadata);
  }

  @Override
  public void compileNesCCode(String queryOutputDir) throws IOException
  {
    sncb.compileNesCCode(queryOutputDir);
  }
}
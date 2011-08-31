package uk.ac.manchester.cs.snee.sncb;

import java.util.HashMap;

import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.Utils;

public class TinyOS_SNCB_Tossim extends TinyOS_SNCB implements SNCB 
{
  private double duration = 0;
  
  public TinyOS_SNCB_Tossim(double duration) throws SNCBException 
  {
    this.duration = duration;
    setup();
  }

  public TinyOS_SNCB_Tossim() throws SNCBException 
  {
    setup();
  }

  @Override
  public void waitForQueryEnd() throws InterruptedException
  {
    if(duration == Double.POSITIVE_INFINITY)
      Thread.currentThread().sleep((long)duration); 
    else
      Thread.currentThread().sleep((long)duration * 1000); 
  }
  
  
}

package uk.ac.manchester.cs.snee.sncb;

public class TinyOS_SNCB_Tossim extends TinyOS_SNCB implements SNCB 
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 8808015438604551042L;
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
    {
      Thread.currentThread();
      Thread.sleep((long)duration); 
    }
    else
    {
      Thread.currentThread();
      Thread.sleep((long)duration * 1000); 
    }
  }
  
  
}

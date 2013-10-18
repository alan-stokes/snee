package uk.ac.manchester.snee.client.queryGenerator;

public class Window
{
  private boolean nowWindow;
  private int range;
  private int slide;
  private timeValue time;
  public static enum timeValue {SECONDS, MINUTES}
  
  public Window()
  {
    nowWindow = true;
  }
  
  public Window(int range, int slide, timeValue time)
  {
    nowWindow = false;
    this.range = range;
    this.slide =slide;
    this.time = time;
  }
  
  public String toString()
  {
    if(nowWindow)
      return "[now] ";
    else
      return "[range " + range + " " + time.toString() + " slide "+ slide + " " + time.toString() +"]";
  }
  
}

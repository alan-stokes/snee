package uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel;

public class Window
{
   private Integer noTuples = 0;
   private Integer windowID;
   
   public Window (int tuples, Integer windowID)
   {
     this.noTuples = tuples;
     this.windowID = windowID;
   }
   
   public Integer getTuples ()
   {
     return noTuples;
   }
   
   public int getWindowID()
   {
     return windowID;
   }
   
   public void setUples(int newTupleLevel)
   {
     this.noTuples = newTupleLevel;
   }
   
   public String toString()
   {
     return this.windowID + "-" + this.noTuples;
   }
}

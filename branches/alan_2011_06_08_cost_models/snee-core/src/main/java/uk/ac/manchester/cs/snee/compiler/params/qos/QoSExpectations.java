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
package uk.ac.manchester.cs.snee.compiler.params.qos;

import java.io.Serializable;
import java.util.Formatter;
import java.util.Locale;
import org.apache.log4j.Logger;

public class QoSExpectations implements Serializable{

    /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 4546548829649007077L;

    @SuppressWarnings("unused")
    private static final Logger logger = Logger.getLogger(QoSExpectations.class.getName());

    private QoSVariable optimizationVariable;

    private QoSOptimizationType optimizationType;

    private int optimizationGoalWeighting = 1;
    
    private long minAcquisitionInterval = -1; //-1 means "not specified"

    private long maxAcquisitionInterval = -1;

    private int acquisitionConstraintWeighting = 1;
    
    private long minDeliveryTime = -1;

    private long maxDeliveryTime = -1;

    private int deliveryTimeConstraintWeighting = 1;
    
    private long minTotalEnergy = -1;

    private long maxTotalEnergy = -1;

    private int totalEnergyConstraintWeighting = 1;
    
    private long minLifetime = -1;

    private long maxLifetime = -1;

    private int lifetimeConstraintWeighting = 1;
    
    private long minBufferingFactor = 1;

    private long maxBufferingFactor = Long.MAX_VALUE;
    
    private int bufferingFactorConstraintWeighting = 1;

    private long queryDuration = 60; // Default, run nesC queries for 60 secs

    public void setOptimizationType(final QoSOptimizationType  
    optimizationType) {
    	this.optimizationType = optimizationType;
    }

    public void setOptimizationType(final String optimizationTypeStr) {
    	this.optimizationType = QoSOptimizationType
			.strToOptimizationType(optimizationTypeStr);
    }

    public void setOptimizationVariable(final String 
    optimizationVariableStr) {
    	this.optimizationVariable = QoSVariable
			.strToOptimizationVariable(optimizationVariableStr);
    }

    public QoSOptimizationType getOptimizationType() {
    	return this.optimizationType;
    }

    public QoSVariable getOptimizationVariable() {
    	return this.optimizationVariable;
    }

	/**
	 * @return the optimizationGoalWeighting
	 */
	public final int getOptimizationGoalWeighting() {
		return optimizationGoalWeighting;
	}

	/**
	 * @param optimizationGoalWeighting the optimizationGoalWeighting to set
	 */
	public final void setOptimizationGoalWeighting(int 
	optimizationGoalWeighting) {
		this.optimizationGoalWeighting = optimizationGoalWeighting;
	}
    
    public long getMaxAcquisitionInterval() {
    	return this.maxAcquisitionInterval;
    }

    public void setMaxAcquisitionInterval(final long maxAcquisitionInterval) {
    	this.maxAcquisitionInterval = maxAcquisitionInterval;
    }

	/**
	 * @return the acquisitionConstraintWeighting
	 */
	public final int getAcquisitionConstraintWeighting() {
		return acquisitionConstraintWeighting;
	}

	/**
	 * @param acquisitionConstraintWeighting the acquisitionConstraintWeighting to set
	 */
	public final void setAcquisitionConstraintWeighting(
			int acquisitionConstraintWeighting) {
		this.acquisitionConstraintWeighting = acquisitionConstraintWeighting;
	}
    
    public long getMaxBufferingFactor() {
	return this.maxBufferingFactor;
    }

    public void setMaxBufferingFactor(final long maxBufferingFactor) {
    	this.maxBufferingFactor = maxBufferingFactor;
    }

	/**
	 * @return the bufferingFactorConstraintWeighting
	 */
	public final int getBufferingFactorConstraintWeighting() {
		return bufferingFactorConstraintWeighting;
	}

	/**
	 * @param bufferingFactorConstraintWeighting the 
	 * 				bufferingFactorConstraintWeighting to set
	 */
	public final void setBufferingFactorConstraintWeighting(
			int bufferingFactorConstraintWeighting) {
		this.bufferingFactorConstraintWeighting = 
			bufferingFactorConstraintWeighting;
	}
    
    public long getMaxDeliveryTime() {
    	return this.maxDeliveryTime;
    }

    public void setMaxDeliveryTime(final long maxDeliveryTime) {
    	this.maxDeliveryTime = maxDeliveryTime;
    }

	/**
	 * @return the deliveryTimeConstraintWeighting
	 */
	public final int getDeliveryTimeConstraintWeighting() {
		return deliveryTimeConstraintWeighting;
	}

	/**
	 * @param deliveryTimeConstraintWeighting the 
	 * deliveryTimeConstraintWeighting to set
	 */
	public final void setDeliveryTimeConstraintWeighting(
			int deliveryTimeConstraintWeighting) {
		this.deliveryTimeConstraintWeighting = 
			deliveryTimeConstraintWeighting;
	}
    
    public long getMaxLifetime() {
    	return this.maxLifetime;
    }

    public void setMaxLifetime(final long maxLifetime) {
    	this.maxLifetime = maxLifetime;
    }

	/**
	 * @return the lifetimeConstraintWeighting
	 */
	public final int getLifetimeConstraintWeighting() {
		return lifetimeConstraintWeighting;
	}

	/**
	 * @param lifetimeConstraintWeighting the lifetimeConstraintWeighting to set
	 */
	public final void setLifetimeConstraintWeighting(int lifetimeConstraintWeighting) {
		this.lifetimeConstraintWeighting = lifetimeConstraintWeighting;
	}

    public long getMaxTotalEnergy() {
    	return this.maxTotalEnergy;
    }

    public void setMaxTotalEnergy(final long maxTotalEnergy) {
    	this.maxTotalEnergy = maxTotalEnergy;
    }

	/**
	 * @return the totalEnergyConstraintWeighting
	 */
	public final int getTotalEnergyConstraintWeighting() {
		return totalEnergyConstraintWeighting;
	}

	/**
	 * @param totalEnergyConstraintWeighting the 
	 * totalEnergyConstraintWeighting to set
	 */
	public final void setTotalEnergyConstraintWeighting(
			int totalEnergyConstraintWeighting) {
		this.totalEnergyConstraintWeighting = totalEnergyConstraintWeighting;
	}
    
    public long getMinAcquisitionInterval() {
    	return this.minAcquisitionInterval;
    }

    public void setMinAcquisitionInterval(final long minAcquisitionInterval) {
    	this.minAcquisitionInterval = minAcquisitionInterval;
    }

    public long getMinBufferingFactor() {
    	return this.minBufferingFactor;
    }

    public void setMinBufferingFactor(final long minBufferingFactor) {
    	this.minBufferingFactor = minBufferingFactor;
    }

    public long getBufferingFactor() {
		if (this.getMinBufferingFactor() == this.getMaxBufferingFactor()) {
		    return this.getMinBufferingFactor();
		} else {
		    return -1;
		}
    }

    public long getMinDeliveryTime() {
    	return this.minDeliveryTime;
    }

    public void setMinDeliveryTime(final long minDeliveryTime) {
    	this.minDeliveryTime = minDeliveryTime;
    }

    public long getMinLifetime() {
    	return this.minLifetime;
    }

    public void setMinLifetime(final long minLifetime) {
    	this.minLifetime = minLifetime;
    }

    public long getMinTotalEnergy() {
    	return this.minTotalEnergy;
    }

    public void setMinTotalEnergy(final long minTotalEnergy) {
		this.minTotalEnergy = minTotalEnergy;
    }

    public void setAcquisitionIntervalRange(final QoSVariableRange r) {
		if (r != null) {
		    this.setMinAcquisitionInterval(r.getMinValue());
		    this.setMaxAcquisitionInterval(r.getMaxValue());
		}
    }

    public void setDeliveryTimeRange(final QoSVariableRange r) {
		if (r != null) {
		    this.setMinDeliveryTime(r.getMinValue());
		    this.setMaxDeliveryTime(r.getMaxValue());
		}
    }

    public void setTotalEnergyRange(final QoSVariableRange r) {
		if (r != null) {
		    this.setMinTotalEnergy(r.getMinValue());
		    this.setMaxTotalEnergy(r.getMaxValue());
		}
    }

    public void setLifetimeRange(final QoSVariableRange r) {
		if (r != null) {
		    this.setMinLifetime(r.getMinValue());
		    this.setMaxLifetime(r.getMaxValue());
		}
    }

    public void setBufferingFactorRange(final QoSVariableRange r) {
		if (r != null) {
		    this.setMinBufferingFactor(r.getMinValue());
		    this.setMaxBufferingFactor(r.getMaxValue());
		}
    }

    public long getQueryDuration() {
		return this.queryDuration;
    }

    public void setQueryDuration(final long queryDuration) {
    	this.queryDuration = queryDuration;
    }

    @Override
    public String toString() {
		final StringBuffer sb = new StringBuffer(
			"Quality of Service Specification\n");
		sb.append("================================\n\n");
		// Send all output to the Appendable object sb
		final Formatter formatter = new Formatter(sb, Locale.UK);
	
		sb.append("\nOptmization goal: " + this.optimizationType 
				+ " " + this.optimizationVariable + "[weighting=" 
				+ this.optimizationGoalWeighting + "]\n\n");
		
		// Explicit argument indices may be used to re-order output.
		formatter.format("%1$10d <= %2$20s <= %3$20d [weighting=%4$1d]\n", 
			new Long(this.minAcquisitionInterval), "acquisition interval", 
			new Long(this.maxAcquisitionInterval), 
			new Integer(this.acquisitionConstraintWeighting));
		formatter.format("%1$10d <= %2$20s <= %3$20d [weighting=%4$1d]\n", 
			new Long(this.minDeliveryTime), "delivery time", 
			new Long(this.maxDeliveryTime),
			new Integer(this.deliveryTimeConstraintWeighting));
		formatter.format("%1$10d <= %2$20s <= %3$20d [weighting=%4$1d]\n", 
			new Long(this.minTotalEnergy), "total energy", 
			new Long(this.maxTotalEnergy),
			new Integer(this.totalEnergyConstraintWeighting));
		formatter.format("%1$10d <= %2$20s <= %3$20d [weighting=%4$1d]\n", 
			new Long(this.minLifetime), "lifetime", 
			new Long(this.maxLifetime),
			new Integer(this.lifetimeConstraintWeighting));
		formatter.format("%1$10d <= %2$20s <= %3$20d [weighting=%4$1d]\n", 
			new Long(this.minBufferingFactor), "buffering factor", 
			new Long(this.maxBufferingFactor),
			new Integer(this.bufferingFactorConstraintWeighting));
		formatter.format("%1$10s    %2$20s  = %3$20d\n", "", "query duration",
			new Long(this.queryDuration));
	
		return sb.toString().replaceAll("-1", "NA");
    }

//    public final void exportToLatex(final String fname, final String queryFileName) {
//  
//    	try {
//	    	// Send all output to the Appendable object sb
//		    final PrintWriter out = new PrintWriter(new BufferedWriter(
//				    new FileWriter(fname)));
//		    out.println("\\documentclass[a4paper]{article}");
//		    out.println("\\begin{document}");
//		    out.println("\\section{Query}");
//		    out.println("\\begin{verbatim}");
//		    
//		    final FileReader in = new FileReader(new File(queryFileName));
//		    final BufferedReader bin = new BufferedReader(in);
//		    String line = null;
//		    while ((line = bin.readLine()) != null) {
//		    	out.println(line);
//		    }
//
//		    out.println("\\end{verbatim}");
//		    out.println("\\section{Quality of Service Specification}");
//	    	
//	    	// Explicit argument indices may be used to re-order output.
//		    out.println("\\begin{tabular}{p{2.5cm}p{4.5cm}p{4cm}p{2cm}}");
//		    out.println("\\hline");
//		    if (this.getOptimizationType() == null 
//		    		|| this.getOptimizationVariable() == null) {
//		    	out.println("No optimization goal & specified & \\\\");
//		    } else {
//			    out.println(this.getOptimizationType() + " &  " 
//			    	+ this.getOptimizationVariable().toString().
//			    	replace("_", " ") + " & & [$w="
//			    	+ this.optimizationGoalWeighting + "$] \\\\");		    	
//		    }
//		    out.println("\\hline");
//		    out.println("$" + this.minAcquisitionInterval + "$ & $\\le$ "
//		    		+ "acquisition interval $\\alpha$ " + " & $\\le "
//		    		+ this.maxAcquisitionInterval + " $ & [$w="
//		    		+ this.acquisitionConstraintWeighting + "$] \\\\");
//		    out.println("$ " + this.minDeliveryTime + "$ & $\\le$ "
//		    		+ "delivery time $\\delta$ " + " & $\\le "
//		    		+ this.maxDeliveryTime + " $ & [$w="
//		    		+ this.deliveryTimeConstraintWeighting + "$] \\\\");
//		    out.println("$ " + this.minTotalEnergy + "$ & $\\le$ "
//		    		+ "total energy $\\epsilon$ " + " & $\\le "
//		    		+ this.maxTotalEnergy + " $ & [$w="
//		    		+ this.totalEnergyConstraintWeighting + "$] \\\\");
//		    out.println("$ " + this.minLifetime + "$ & $\\le$ "
//		    		+ "lifetime $\\lambda$ " + " & $\\le "
//		    		+ this.maxLifetime + " $ & [$w="
//		    		+ this.lifetimeConstraintWeighting + "$]\\\\");		    
//		    out.println("$ " + this.minBufferingFactor + "$ & $\\le$ "
//		    		+ "buffering factor $\\beta$ " + " & $\\le "
//		    		+ this.maxBufferingFactor + " $ & [$w="
//		    		+ this.bufferingFactorConstraintWeighting + "$]\\\\");
//		    out.println("query duration & $= "
//		    		+ this.queryDuration + " $ & \\\\");
//		    out.println("\\hline");
//		    out.println("\\end{tabular}");
//		    
//		    out.println("\\end{document}");
//	    	out.close();
//    	} catch (Exception e) {
//    		Utils.handleCriticalException(new OptimizationException(
//    				"Error while exporting QoS specification to Latex"));
//    	}
//    }


}

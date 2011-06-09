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
package uk.ac.manchester.cs.snee.sncb.tos;

import uk.ac.manchester.cs.snee.common.graph.Edge;
import uk.ac.manchester.cs.snee.common.graph.EdgeImplementation;

public class Wiring extends EdgeImplementation implements Edge {

    //user = source
    //provider = dest
    String interfaceType;

    String typeParameter;

    String userAsName;

    String providerAsName;

    public Wiring(final String id, final String user, final String provider) {
	super(id, user, provider);
    }

    public String getProvider() {
	return this.getDestID();
    }

    public String getProviderAsName() {
	return this.providerAsName;
    }

    public void setInterfaceType(final String interfaceType) {
	this.interfaceType = interfaceType;
    }

    public void setTypeParameter(final String typeParameter) {
	this.typeParameter = typeParameter;
    }

    public void setUserAsName(final String userAsName) {
	this.userAsName = userAsName;
    }

    public void setProviderAsName(final String providerAsName) {
	this.providerAsName = providerAsName;
    }

    public String getInterfaceType() {
	return this.interfaceType;
    }

    public String getTypeParameter() {
	return this.typeParameter;
    }

    public String getUser() {
	return this.getSourceID();
    }

    public String getUserAsName() {
	return this.userAsName;
    }

    @Override
    public boolean equals(final Object obj) {
	if (this == obj) {
	    return true;
	}
	if (obj == null) {
	    return false;
	}
	if (this.getClass() != obj.getClass()) {
	    return false;
	}
	final Wiring other = (Wiring) obj;
	if (this.getDestID() == null) {
	    if (other.getDestID() != null) {
		return false;
	    }
	} else if (!this.getDestID().equals(other.getDestID())) {
	    return false;
	}
	if (this.providerAsName == null) {
	    if (other.providerAsName != null) {
		return false;
	    }
	} else if (!this.providerAsName.equals(other.providerAsName)) {
	    return false;
	}
	if (this.getSourceID() == null) {
	    if (other.getSourceID() != null) {
		return false;
	    }
	} else if (!this.getSourceID().equals(other.getSourceID())) {
	    return false;
	}
	if (this.userAsName == null) {
	    if (other.userAsName != null) {
		return false;
	    }
	} else if (!this.userAsName.equals(other.userAsName)) {
	    return false;
	}
	return true;
    }

    @Override
    public Wiring clone() {
	final Wiring w = new Wiring(this.getID(), this.getSourceID(), this
		.getDestID());
	w.setInterfaceType(this.interfaceType);
	w.setProviderAsName(this.providerAsName);
	w.setTypeParameter(this.typeParameter);
	w.setUserAsName(this.userAsName);
	return w;
    }
}

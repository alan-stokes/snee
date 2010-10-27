/* $Id$
 * Copyright (c) 2006 Intel Corporation
 * All rights reserved.
 *
 * This file is distributed under the terms in the attached INTEL-LICENSE     
 * file. If you do not find these files, copies can be found by writing to
 * Intel Research Berkeley, 2150 Shattuck Avenue, Suite 1300, Berkeley, CA, 
 * 94704.  Attention:  Intel License Inquiry.
 */
/**
 * The micaZ doesn't have any built-in sensors - the DemoSensor returns
 * a constant value of 0xbeef, or just reads the ground value for the
 * stream sensor.
 *
 * @author Philip Levis
 * @authod David Gay
 */

generic module RandomSensorC()
{
  provides interface Init;
  provides interface Read<uint16_t>;
}
implementation {

  command error_t Init.init() {
    return SUCCESS;
  }
  
  task void readTask() {
    int val = rand();
    
    signal Read.readDone(SUCCESS, (uint16_t)val);
  }

  command error_t Read.read() {
    post readTask();
    return SUCCESS;
  }
}

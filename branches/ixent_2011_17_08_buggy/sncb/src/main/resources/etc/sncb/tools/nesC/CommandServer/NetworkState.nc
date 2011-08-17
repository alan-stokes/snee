#include "NetworkState.h"

interface NetworkState {
  command void setNetworkFailureState();
  event void changed(network_state_t state);
}


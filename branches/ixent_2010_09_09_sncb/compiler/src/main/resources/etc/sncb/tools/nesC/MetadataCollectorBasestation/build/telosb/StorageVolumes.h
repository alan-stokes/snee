#ifndef __STORAGE_VOLUME_H__
#define __STORAGE_VOLUME_H__

#include "Stm25p.h"

#define VOLUME_GOLDENIMAGE 0
#define VOLUME_DELUGE1 1
#define VOLUME_DELUGE2 2
#define VOLUME_DELUGE3 3

static const stm25p_volume_info_t STM25P_VMAP[ 4 ] = {
    { base : 15, size : 1 },
    { base : 0, size : 1 },
    { base : 1, size : 1 },
    { base : 2, size : 1 },
};

#endif

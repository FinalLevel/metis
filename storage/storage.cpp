///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis storage server control class implementation
///////////////////////////////////////////////////////////////////////////////

#include "storage.hpp"
#include "metis_log.hpp"

using namespace fl::metis;

Storage::Storage(const char *path, const double minFree, const TSize maxSliceSize)
	: _sliceManager(path, minFree, maxSliceSize)
{
	if (!_sliceManager.loadIndex(_index)) {
		log::Fatal::L("Can't load index\n");
		throw std::exception();
	}
}

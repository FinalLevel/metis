#pragma once
#ifndef __FL_METIS_STORAGE_HPP
#define	__FL_METIS_STORAGE_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis storage server control class
///////////////////////////////////////////////////////////////////////////////

#include "range_index.hpp"
#include "slice.hpp"

namespace fl {
	namespace metis {
		class Storage
		{
		public:
			Storage(const char *path, const double minFree, const TSize maxSliceSize);
		private:
			SliceManager _sliceManager;
			Index _index;
		};
	};
};

#endif	// __FL_METIS_STORAGE_HPP

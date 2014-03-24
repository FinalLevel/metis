#pragma once
#ifndef __FL_METIS_STORAGE_SLICE_HPP
#define	__FL_METIS_STORAGE_SLICE_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Storage slice managment class
///////////////////////////////////////////////////////////////////////////////

#include "file.hpp"
#include <vector>
#include <memory>
#include <limits>
#include <string>
#include "../types.hpp"

namespace fl {
	namespace metis {
		using fl::fs::File;
		
		static const uint16_t BLOCK_SIZE = 4096;
		typedef uint16_t TBlockNumber;
		typedef uint16_t TSliceID;
		struct ItemIndexEntry
		{
			TSliceID sliceID;
			TBlockNumber startBlook;
			TItemModTime modTime;
		} __attribute__((packed));
		
		static const uint16_t MAX_BLOCK_NUMBER = std::numeric_limits<TBlockNumber>::max() - 1;
		static const uint32_t MAX_SLICE_SIZE = BLOCK_SIZE * MAX_BLOCK_NUMBER;

		class Slice
		{
		public:
		private:
			TBlockNumber _lastBlock;
			File _sliceData;
			File _sliceIndex;
		};
		typedef std::shared_ptr<class Slice> TSlicePtr;
		
		class SliceManager
		{
		public:
			SliceManager(const char *path);
			bool add(const char *data, const ItemHeader &itemHeader);
			bool init();
		private:
			std::string _path;
			typedef std::vector<TSlicePtr> TSliceVector;
			TSliceVector _slices;
			TSlicePtr _writeSlice;
		};
	};
};

#endif	// __FL_METIS_STORAGE_SLICE_HPP

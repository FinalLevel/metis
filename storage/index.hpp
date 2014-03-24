#pragma once
#ifndef __FL_METIS_STORAGE_INDEX_HPP
#define	__FL_METIS_STORAGE_INDEX_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Storage Index class
///////////////////////////////////////////////////////////////////////////////

#include "../config.h"
#ifdef HAVE_CXX11
	#include <unordered_map>
	using std::unordered_map;
#else
	#include <boost/unordered_map.hpp>
	using boost::unordered_map;
#endif
	
#include "../types.hpp"
#include "slice.hpp"
	
namespace fl {
	namespace metis {
		
		class TopLevelIndex
		{
		public:
			TopLevelIndex(const char *path);
			bool init();
		private:
			SliceManager _sliceManager;
			
			typedef unordered_map<TItemKey, ItemIndexEntry> TItemIndex;
			typedef unordered_map<TSubLevel, TItemIndex> TSubLevelIndex;
			TSubLevelIndex _subLevels;
			
		};
		typedef std::shared_ptr<class TopLevelIndex> TTopLevelIndexPtr;
		
		class Index
		{
		public:
			Index(const std::string &path, const double minFree);
			bool init();
		private:
			bool _recalcSpace();
			std::string _path;
			double _minFree;
			uint64_t _leftSpace;
			typedef unordered_map<TLevel, TTopLevelIndexPtr> TIndex;
			TIndex _index;
		};
	};
};

#endif	// __FL_METIS_STORAGE_INDEX_HPP

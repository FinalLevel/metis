#pragma once
#ifndef __FL_METIS_STORAGE_RANGE_INDEX_HPP
#define	__FL_METIS_STORAGE_RANGE_INDEX_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description:  Range index controlling class
///////////////////////////////////////////////////////////////////////////////

#include "config.h"
#ifdef HAVE_CXX11
	#include <unordered_map>
	using std::unordered_map;
#else
	#include <boost/unordered_map.hpp>
	using boost::unordered_map;
#endif

#include <memory>
	
#include "../types.hpp"
#include "mutex.hpp"

namespace fl {
	namespace metis {
		using fl::threads::Mutex;
		using fl::threads::AutoMutex;
		
		
		class Range
		{
		public:
			struct Entry
			{
				Entry() = default;
				Entry(const IndexEntry &ie)
					: pointer(ie.pointer), size(ie.header.size), timeTag(ie.header.timeTag)
				{
				}
				ItemPointer pointer;
				TSize size;
				ModTimeTag timeTag;
			};

			Range();
			bool find(const TItemKey itemKey, Entry &ie);
			void add(const IndexEntry &ie)
			{
				AutoMutex autoSync(&_sync);
				addNoLock(ie);
			}
			void addNoLock(const IndexEntry &ie);
			bool remove(const ItemHeader &itemHeader);
		private:
			TItemKey _minID;
			TItemKey _maxID;
			typedef unordered_map<TItemKey, Entry> TItemHash;
			TItemHash _items;
			Mutex _sync;
		};
		typedef std::shared_ptr<Range> TRangePtr;
		
		class Index
		{
		public:
			bool find(const TRangeID rangeID, const TItemKey itemKey, Range::Entry &ie);
			void add(const IndexEntry &ie);
			void addNoLock(const IndexEntry &ie);
			bool remove(const ItemHeader &itemHeader);
		private:
			typedef unordered_map<TRangeID, TRangePtr> TRangeHash;
			TRangeHash _ranges;
			Mutex _sync;
		};
	};
};

#endif	// __FL_METIS_STORAGE_RANGE_INDEX_HPP

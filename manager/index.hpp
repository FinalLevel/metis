#pragma once
#ifndef __FL_MANAGER_INDEX_HPP
#define	__FL_MANAGER_INDEX_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Index control classes
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
#include "mysql.hpp"

	
namespace fl {
	namespace metis {
		using fl::db::Mysql;
		
		class Range
		{
		public:
			Range(const TRangeID rangeID, const TItemKey rangeIndex, const TServerID managerID);
		private:
			TRangeID _rangeID;
			TItemKey _rangeIndex;
			TServerID _managerID;
		};
		
		class RangeIndex
		{
		public:
			typedef uint8_t TStatus;
			typedef uint32_t TIndexID;
			RangeIndex(const TIndexID id, const TStatus status);
			const TIndexID id() const
			{
				return _id;
			}
			void add(const TRangeID rangeID, const TItemKey rangeIndex, const TServerID managerID);
		private:
			TIndexID _id;
			TStatus _status;
			typedef unordered_map<TItemKey, Range> TRangeMap;
			TRangeMap _ranges;
		};
		typedef std::shared_ptr<RangeIndex> TRangeIndexPtr;
		
		class Index
		{
		public:
			bool loadAll(Mysql &sql);
		private:
			bool _loadIndex(Mysql &sql);
			bool _loadIndexRanges(Mysql &sql);
			void _add(const TLevel level, const TSubLevel sublevel, TRangeIndexPtr &rangeIndex);
			
			typedef unordered_map<TSubLevel, TRangeIndexPtr> TSubLevelMap;
			typedef unordered_map<TLevel, TSubLevelMap> TLevelMap;
			TLevelMap _index;
			
			typedef unordered_map<RangeIndex::TIndexID, TRangeIndexPtr> TRangeMap;
			TRangeMap _indexRanges;
		};
	};
};

#endif	// __FL_MANAGER_INDEX_HPP

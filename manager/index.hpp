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
#include "mutex.hpp"

	
namespace fl {
	namespace metis {
		using fl::db::Mysql;
		using fl::db::MysqlResult;
		using fl::threads::Mutex;
		using fl::threads::AutoMutex;
		
		class Range
		{
		public:
			Range(const TRangeID rangeID, const TItemKey rangeIndex, const TServerID managerID);
			TRangeID rangeID() const
			{
				return _rangeID;
			}
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
			RangeIndex(MysqlResult *res);
			const TIndexID id() const
			{
				return _id;
			}
			void addNL(const TRangeID rangeID, const TItemKey rangeIndex, const TServerID managerID);
			void update(RangeIndex *src);
			bool fillAndAdd(ItemHeader &item, bool &needNotify);
		private:
			TIndexID _id;
			TStatus _status;
			typedef unordered_map<TItemKey, Range> TRangeMap;
			TRangeMap _ranges;
			Mutex _sync;
			TItemKey _calcRangeIndex(const TItemKey itemKey);
			TItemKey _rangeSize;
		};
		typedef std::shared_ptr<RangeIndex> TRangeIndexPtr;
		
		class Index
		{
		public:
			Index(class Config *config);
			bool loadAll(Mysql &sql);
			bool parseURL(const std::string &host, const std::string &fileName, ItemHeader &item, TCrc &crc);
			bool fillAndAdd(ItemHeader &item, bool &needNotify);
			bool addLevel(const TLevel level, const TSubLevel subLevel);
			bool loadLevel(const TLevel level, const TSubLevel subLevel, Mysql &sql);
		private:
			bool _loadIndex(Mysql &sql);
			bool _loadIndexRanges(Mysql &sql);
			void _add(const TLevel level, const TSubLevel sublevel, TRangeIndexPtr &rangeIndex);
			
			typedef unordered_map<TSubLevel, TRangeIndexPtr> TSubLevelMap;
			typedef unordered_map<TLevel, TSubLevelMap> TLevelMap;
			TLevelMap _index;
			
			typedef unordered_map<RangeIndex::TIndexID, TRangeIndexPtr> TRangeMap;
			TRangeMap _indexRanges;
			Mutex _sync;
			
			class Config *_config;
		};
	};
};

#endif	// __FL_MANAGER_INDEX_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Index control classes implementation
///////////////////////////////////////////////////////////////////////////////

#include "index.hpp"
#include "metis_log.hpp"


using namespace fl::metis;

Range::Range(const TRangeID rangeID, const TItemKey rangeIndex, const TServerID managerID)
	: _rangeID(rangeID), _rangeIndex(rangeIndex), _managerID(managerID)
{
}


RangeIndex::RangeIndex(const TIndexID id, const TStatus status)
	: _id(id), _status(status)
{
}

void RangeIndex::add(const TRangeID rangeID, const TItemKey rangeIndex, const TServerID managerID)
{ 
	Range range(rangeID, rangeIndex, managerID);
	auto res = _ranges.emplace(rangeIndex, range);
	if (!res.second)
		res.first->second = range;
}

namespace EIndexFlds
{
	enum EIndexFlds
	{
		ID,
		LEVEL,
		SUBLEVEL,
		STATUS,
	};
};
const char * const INDEX_SQL = "SELECT id, level, subLevel, status FROM `index`";


bool Index::_loadIndex(Mysql &sql)
{
	auto res = sql.query(INDEX_SQL);
	if (!res)	{
		log::Error::L("Cannot load information about index top levels\n");
		return false;
	}
	while (res->next()) {
		TRangeIndexPtr rangeIndex( new RangeIndex(
			res->get<RangeIndex::TIndexID>(EIndexFlds::ID),
			res->get<RangeIndex::TStatus>(EIndexFlds::STATUS)
		));
		_add(res->get<TLevel>(EIndexFlds::LEVEL), res->get<TSubLevel>(EIndexFlds::SUBLEVEL), rangeIndex);
	}
	log::Info::L("Load %u indexes\n", _indexRanges.size());
	return true;	
}

namespace EIndexRangeFlds
{
	enum EIndexRangeFlds
	{
		ID,
		INDEXID,
		RANGE_INDEX,
		MANAGERID
	};
};

const char * const INDEX_RANGE_SQL = "SELECT id, indexID, rangeIndex, managerID FROM index_range";

bool Index::_loadIndexRanges(Mysql &sql)
{
	auto res = sql.query(INDEX_RANGE_SQL);
	if (!res)	{
		log::Error::L("Cannot load information about index ranges\n");
		return false;
	}
	uint32_t c = 0;
	while (res->next()) {
		auto indexID = res->get<RangeIndex::TIndexID>(EIndexRangeFlds::INDEXID);
		auto f = _indexRanges.find(indexID);
		if (f == _indexRanges.end()) {
			log::Warning::L("Can't find index %u\n", indexID);
			continue;
		}
		c++;
		f->second->add(
			res->get<TRangeID>(EIndexRangeFlds::ID), 
			res->get<TItemKey>(EIndexRangeFlds::RANGE_INDEX),
			res->get<TServerID>(EIndexRangeFlds::MANAGERID)
		);
	}
	log::Info::L("Load %u index ranges\n", c);
	return true;
}

void Index::_add(const TLevel level, const TSubLevel sublevel, TRangeIndexPtr &rangeIndex)
{
	static TSubLevelMap empty;
	auto res = _index.emplace(level, empty);
	auto sublevelRes = res.first->second.emplace(sublevel, rangeIndex);
	if (!sublevelRes.second)
		sublevelRes.first->second = rangeIndex;
	
	auto rangeResult = _indexRanges.emplace(rangeIndex->id(), rangeIndex);
	if (!rangeResult.second)
		rangeResult.first->second = rangeIndex;
}

bool Index::loadAll(Mysql &sql)
{
	if (!_loadIndex(sql))
		return false;
	
	if (!_loadIndexRanges(sql))
		return false;

	return true;
}

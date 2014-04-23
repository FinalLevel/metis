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
#include "config.hpp"
#include "util.hpp"


using namespace fl::metis;
using namespace fl::utils;
using fl::db::ESC;

Range::Range(const TRangeID rangeID, const TItemKey rangeIndex, const TServerID managerID)
	: _rangeID(rangeID), _rangeIndex(rangeIndex), _managerID(managerID)
{
}

namespace EIndexFlds
{
	enum EIndexFlds
	{
		ID,
		LEVEL,
		SUBLEVEL,
		STATUS,
		RANGE_SIZE,
	};
};
const char * const INDEX_SQL = "SELECT id, level, subLevel, status, rangeSize FROM `index`";


RangeIndex::RangeIndex(MysqlResult *res)
	: _id(res->get<decltype(_id)>(EIndexFlds::ID)), 
		_status(res->get<decltype(_status)>(EIndexFlds::STATUS)),
		_rangeSize(res->get<decltype(_rangeSize)>(EIndexFlds::RANGE_SIZE))
{
}

void RangeIndex::addNL(const TRangeID rangeID, const TItemKey rangeIndex, const TServerID managerID)
{ 
	Range range(rangeID, rangeIndex, managerID);
	auto res = _ranges.emplace(rangeIndex, range);
	if (!res.second)
		res.first->second = range;
}

TItemKey RangeIndex::_calcRangeIndex(const TItemKey itemKey)
{
	return itemKey / _rangeSize;
}

bool RangeIndex::fillAndAdd(ItemHeader &item, bool &needNotify)
{
	TItemKey rangeIndex = _calcRangeIndex(item.itemKey);
	AutoMutex autoSync(&_sync);
	auto range = _ranges.find(rangeIndex);
	if (range == _ranges.end()) {
		return false;
	} else {
		item.rangeID = range->second.rangeID();
		return true;
	}
}


void RangeIndex::update(RangeIndex *src)
{
	_status = src->_status;
}

Index::Index(class Config *config)
	: _config(config)
{
}

bool Index::_loadIndex(Mysql &sql)
{
	auto res = sql.query(INDEX_SQL);
	if (!res)	{
		log::Error::L("Cannot load information about index top levels\n");
		return false;
	}
	while (res->next()) {
		TRangeIndexPtr rangeIndex( new RangeIndex(res.get()));
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
		f->second->addNL(
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
		sublevelRes.first->second->update(rangeIndex.get());
	
	_indexRanges.emplace(rangeIndex->id(), rangeIndex);
}

bool Index::loadAll(Mysql &sql)
{
	if (!_loadIndex(sql))
		return false;
	
	if (!_loadIndexRanges(sql))
		return false;

	return true;
}

bool Index::parseURL(const std::string &host, const std::string &fileName, ItemHeader &item, TCrc &crc)
{
	bzero(&item, sizeof(item));
	const char *pFileName = fileName.c_str();
	char *pEnd = NULL;
	item.level = strtoul(pFileName, &pEnd, 10);
	if (!pEnd || (*pEnd != '/')) {
		log::Warning::L("Can't find level in %s\n", fileName.c_str());
		return false;
	}
	pFileName = pEnd + 1;
	item.subLevel = strtoul(pFileName, &pEnd, 10);
	if (!pEnd || (*pEnd != '/')) {
		log::Warning::L("Can't find sublevel in %s\n", fileName.c_str());
		return false;
	}
	pFileName = pEnd + 1;
	item.itemKey = convertStringTo<decltype(item.itemKey)>(pFileName, &pEnd, 10);
	crc = 0;
	if (!pEnd || (*pEnd != '_'))
		return true;
	pFileName = pEnd + 1;
	crc = convertStringTo<TCrc>(pFileName, &pEnd, 16);
	return true;
}

bool Index::fillAndAdd(ItemHeader &item, bool &needNotify)
{
	AutoMutex autoSync(&_sync);
	auto level = _index.find(item.level);
	if (level == _index.end()) {
		log::Warning::L("Can't find level %u\n", item.level);
		return false;
	}
	auto subLevel = level->second.find(item.subLevel);
	if (subLevel == level->second.end()) {
		log::Warning::L("Can't find level %u / subLevel %u\n", item.level, item.subLevel);
		return false;
	}
	TRangeIndexPtr rangeIndex = subLevel->second;
	autoSync.unLock();
	return rangeIndex->fillAndAdd(item, needNotify);
}

bool Index::addLevel(const TLevel levelID, const TSubLevel subLevelID)
{
	AutoMutex autoSync(&_sync);
	auto level = _index.find(levelID);
	if (level != _index.end()) { // create new level
		auto subLevel = level->second.find(subLevelID);
		if (subLevel != level->second.end()) {
			log::Error::L("Level %u / subLevel %s is already exists\n", levelID, subLevelID);
			return false;
		}
	}
	autoSync.unLock();
	
	Mysql sql;
	if (!_config->connectDb(sql)) {
		log::Error::L("Index::addLevel: Cannot connect to db, check db parameters\n");
		return false;
	}
	auto sqlBuf = sql.createQuery();
	sqlBuf << "INSERT IGNORE INTO `index` SET level=" << ESC << levelID << ",subLevel=" << ESC << subLevelID;
	if (!sql.execute(sqlBuf))
		return false;
	
	return loadLevel(levelID, subLevelID, sql);
}

bool Index::loadLevel(const TLevel level, const TSubLevel subLevel, Mysql &sql)
{
	auto sqlBuf = sql.createQuery();
	sqlBuf << INDEX_SQL << " WHERE level=" << ESC << level << ",subLevel=" << ESC << subLevel;
	auto res = sql.query(sqlBuf);
	if (!res || !res->next()) {
		log::Error::L("Index::addLevel: Cannot load index.id from db\n");
		return false;
	}
	AutoMutex autoSync(&_sync);
	TRangeIndexPtr rangeIndex(new RangeIndex(res.get()));
	_add(level, subLevel, rangeIndex);
	return true;
}


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
#include "cluster_manager.hpp"
#include "event_thread.hpp"
#include "time.hpp"

using namespace fl::metis;
using namespace fl::utils;
using fl::db::ESC;
using fl::chrono::Time;

namespace EIndexRangeFlds
{
	enum EIndexRangeFlds
	{
		ID,
		INDEXID,
		RANGE_INDEX,
		MANAGERID,
		STORAGES
	};
};

const char * const INDEX_RANGE_SQL = "SELECT id, indexID, rangeIndex, managerID, storageIDs FROM index_range";

Range::Range(MysqlResult *res, ClusterManager &clusterManager)
	: _rangeID(res->get<decltype(_rangeID)>(EIndexRangeFlds::ID)), 
		_rangeIndex(res->get<decltype(_rangeIndex)>(EIndexRangeFlds::RANGE_INDEX)), 
		_managerID(res->get<decltype(_managerID)>(EIndexRangeFlds::MANAGERID))
{
	TServerIDList storageIds;
	if (explode<TServerIDList::value_type>(res->get(EIndexRangeFlds::STORAGES), storageIds)) {
		clusterManager.findStorages(storageIds, _storages);
	}
}

void Range::update(Range *src)
{
	_managerID = src->_managerID;
}

StorageNode *Range::getPutStorage(const TSize size, Config *config, class ClusterManager &clusterManager, 	bool &wasAdded)
{
	for (auto s = _storages.begin(); s != _storages.end(); s++) {
		if ((*s)->canPut(size))
			return s->get();
	}
	//TODO: Add new storage
	return NULL;
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

void RangeIndex::addNL(TRangePtr &range)
{ 
	auto res = _ranges.emplace(range->rangeIndex(), range);
	if (!res.second)
		res.first->second->update(range.get());
}

TItemKey RangeIndex::calcRangeIndex(const TItemKey itemKey)
{
	return itemKey / _rangeSize;
}

bool RangeIndex::find(const TItemKey rangeIndex, TRangePtr &range)
{
	AutoMutex autoSync(&_sync);
	auto f = _ranges.find(rangeIndex);
	if (f == _ranges.end()) {
		return false;		
	} else {
		range = f->second;
		return true;
	}
}


void RangeIndex::update(RangeIndex *src)
{
	_status = src->_status;
}

IndexManager::IndexManager(class Config *config)
	: _config(config)
{
}

bool IndexManager::_loadIndex(Mysql &sql)
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

void IndexManager::_addRange(TRangePtr &range)
{
	_ranges.emplace(range->rangeID(), range);
}

bool IndexManager::_loadIndexRanges(Mysql &sql, ClusterManager &clusterManager)
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
		TRangePtr range(new Range(res.get(), clusterManager));
		f->second->addNL(range);
		_addRange(range);
	}
	log::Info::L("Load %u index ranges\n", c);
	return true;
}

void IndexManager::_add(const TLevel level, const TSubLevel sublevel, TRangeIndexPtr &rangeIndex)
{
	static TSubLevelMap empty;
	auto res = _index.emplace(level, empty);
	auto sublevelRes = res.first->second.emplace(sublevel, rangeIndex);
	if (!sublevelRes.second)
		sublevelRes.first->second->update(rangeIndex.get());
	
	_indexRanges.emplace(rangeIndex->id(), rangeIndex);
}

bool IndexManager::loadAll(Mysql &sql, ClusterManager &clusterManager)
{
	if (!_loadIndex(sql))
		return false;
	
	if (!_loadIndexRanges(sql, clusterManager))
		return false;

	return true;
}

bool IndexManager::parseURL(const std::string &host, const std::string &fileName, ItemHeader &item, TCrc &crc)
{
	bzero(&item, sizeof(item));
	const char *pFileName = fileName.c_str();
	char *pEnd = NULL;
	if (*pFileName == '/')
		pFileName++;
	item.level = strtoul(pFileName, &pEnd, 10);
	if (!pEnd || (*pEnd != '/')) {
		log::Warning::L("Can't find level in %s\n", fileName.c_str());
		return false;
	}
	pFileName = pEnd + 1;
	item.subLevel = strtoul(pFileName, &pEnd, 10);
	if (*pEnd != '/') {
		return true;
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

bool RangeIndex::loadRange(TRangePtr &range, const TItemKey rangeIndex, IndexManager *index, 
	ClusterManager &clusterManager, Mysql &sql)
{
	auto sqlQuery = sql.createQuery();
	sqlQuery << INDEX_RANGE_SQL << " WHERE indexID=" << ESC << _id << " AND rangeIndex=" << ESC << rangeIndex;
	auto res = sql.query(sqlQuery);
	if (!res || !res->next())	{
		log::Error::L("Cannot load information about index ranges\n");
		return false;
	}
	range.reset(new Range(res.get(), clusterManager));
	add(range);
	index->addRange(range);
	return true;
}

bool IndexManager::findAndFill(ItemHeader &item, TRangePtr &range)
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
	TRangeIndexPtr rangesIndex = subLevel->second;
	autoSync.unLock();
	
	auto rangeIndex = rangesIndex->calcRangeIndex(item.itemKey);
	if (rangesIndex->find(rangeIndex, range)) {
		item.rangeID = range->rangeID();
		return true;
	}
	else 
		return false;
}

bool IndexManager::fillAndAdd(ItemHeader &item, TRangePtr &range, ClusterManager &clusterManager, bool &wasAdded)
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
	TRangeIndexPtr rangesIndex = subLevel->second;
	autoSync.unLock();
	
	auto rangeIndex = rangesIndex->calcRangeIndex(item.itemKey);
	if (!rangesIndex->find(rangeIndex, range)) {
		Mysql sql;
		if (!_config->connectDb(sql)) {
			log::Error::L("RangeIndex::fillAndAdd: Cannot connect to db, check db parameters\n");
			return false;
		}
		TServerIDList storageIDs;
		if (!clusterManager.findFreeStorages(_config->minimumCopies(), storageIDs)) {
			log::Error::L("RangeIndex::fillAndAdd: Cannot find free storages for a new range\n");
			return false;
		}
		TServerID managerID = clusterManager.findFreeManager();
		auto sqlBuf = sql.createQuery();
		sqlBuf << "INSERT IGNORE INTO `index_range` SET indexID=" << ESC << rangesIndex->id() << ",rangeIndex=" 
			<< ESC << rangeIndex << ",storageIDs=" << ESC << storageIDs << ",managerID=" << ESC << managerID;
		if (!sql.execute(sqlBuf))
			return false;

		wasAdded = true;
		if (!rangesIndex->loadRange(range, rangeIndex, this, clusterManager, sql))
			return false;
	}
	item.rangeID = range->rangeID();
	item.timeTag = genNewTimeTag();
	return true;
}

uint32_t IndexManager::_curOperation  = 0;

ModTimeTag IndexManager::genNewTimeTag()
{
	Time curTime;
	ModTimeTag tag;
	tag.modTime = curTime.unix();
	tag.op = __sync_add_and_fetch(&_curOperation, 1);
	return tag;
}

bool IndexManager::addLevel(const TLevel levelID, const TSubLevel subLevelID)
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

bool IndexManager::loadLevel(const TLevel level, const TSubLevel subLevel, Mysql &sql)
{
	auto sqlBuf = sql.createQuery();
	sqlBuf << INDEX_SQL << " WHERE level=" << ESC << level << " AND subLevel=" << ESC << subLevel;
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

StorageNode *IndexManager::getPutStorage(const TRangeID rangeID, const TSize size, class ClusterManager &clusterManager,
	bool &wasAdded)
{
	wasAdded = false;
	TRangePtr range;
	AutoMutex autoSync(&_sync);
	auto f = _ranges.find(rangeID);
	if (f == _ranges.end()) {
		log::Fatal::L("Try find put storage for unknown rangeID %u\n", rangeID);
		return NULL;
	}
	range = f->second;
	autoSync.unLock();
	return range->getPutStorage(size, _config, clusterManager, wasAdded);
}


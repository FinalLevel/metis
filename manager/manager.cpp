///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Manager server control class implementation
///////////////////////////////////////////////////////////////////////////////

#include "manager.hpp"
#include "metis_log.hpp"
#include "storage_cmd_event.hpp"

using namespace fl::metis;

Manager::Manager(Config* config)
	: _config(config), _indexManager(config), 
		_cache(config->cacheSize(), config->itemHeadersCacheSize(), config->itemsInLine(), config->minHitsToCache()), 
		_rangeIndexCheck(NULL)
{
	_timeThread = new fl::threads::TimeThread(5 * 60);
	_timeThread->addEveryTick(new fl::threads::TimeTask<Manager>(this, &Manager::timeTic));
	if (!_timeThread->create())
	{
		log::Fatal::L("Can't create a time thread\n");
		throw std::exception();
	}	
}

bool Manager::timeTic(fl::chrono::ETime &curTime)
{
	_cache.recycle();
	return true;
}

bool Manager::loadAll()
{
	Mysql sql;
	if (!_config->connectDb(sql)) {
		log::Error::L("Manager: Cannot connect to db, check db parameters\n");
		return false;
	}
	if (!_clusterManager.loadAll(sql)) {
		return false;
	}
	if (!_indexManager.loadAll(sql, _clusterManager)) {
		return false;
	}
	return true;
}

bool Manager::addLevel(const TLevel level, const TSubLevel subLevel)
{
	if (!_indexManager.addLevel(level, subLevel))
		return false;
	return true;
}


bool Manager::fillAndAdd(ItemHeader &item, TRangePtr &range, bool &wasAdded)
{
	wasAdded = false;
	if (!_indexManager.fillAndAdd(item, range, _clusterManager, wasAdded))
		return false;
	
	return true;
}


class StorageNode *Manager::getStorageForCopy(const TRangeID rangeID, const TSize size, TStorageList &storages)
{
	bool wasAdded = false;
	auto storage = _indexManager.getStorageForCopy(rangeID, size, _clusterManager, storages, wasAdded);
	if (!storage)
		return NULL;
	
	return storage;

}

bool Manager::getPutStorages(const TRangeID rangeID, const TSize size, TStorageList &storages)
{
	bool wasAdded = false;
	if (!_indexManager.getPutStorages(rangeID, size, _clusterManager, storages, wasAdded))
		return false;
	
	return true;
}

bool Manager::startRangesChecking(EPollWorkerThread *thread)
{
	_rangeIndexCheck = new StorageCMDRangeIndexCheck(this, thread);
	return _rangeIndexCheck->start();
}


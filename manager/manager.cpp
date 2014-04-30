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

using namespace fl::metis;

Manager::Manager(Config* config)
	: _config(config), _indexManager(config)
{
	
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

bool Manager::findAndFill(ItemHeader &item, TRangePtr &range)
{
	if (!_indexManager.findAndFill(item, range))
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

StorageNode *Manager::getPutStorage(const TRangeID rangeID, const TSize size)
{
	bool wasAdded = false;
	StorageNode *node = _indexManager.getPutStorage(rangeID, size, _clusterManager, wasAdded);
	if (!node)
		return NULL;
	
	return node;
}

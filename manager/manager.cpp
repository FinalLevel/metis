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
	: _config(config), _index(config)
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
	if (!_index.loadAll(sql)) {
		return false;
	}
	return true;
}

bool Manager::addLevel(const TLevel level, const TSubLevel subLevel)
{
	if (!_index.addLevel(level, subLevel))
		return false;
	return true;
}

bool Manager::fillAndAdd(ItemHeader &item)
{
	bool needNotify = false;
	if (!_index.fillAndAdd(item, needNotify))
		return false;
	return true;
}

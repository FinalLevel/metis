///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis cluster control class implementation
///////////////////////////////////////////////////////////////////////////////

#include "cluster_manager.hpp"
#include "../metis_log.hpp"

using namespace fl::metis;

namespace EManagerFlds
{
	enum EManagerFlds
	{
		ID,
		IP,
		PORT,
		STATUS,
	};
};

const char * const MANAGER_SQL = "SELECT id, ip, port, status FROM manager";

ManagerNode::ManagerNode(MysqlResult *res)
	: _id(res->get<decltype(_id)>(EManagerFlds::ID)), 
		_ip(Socket::ip2Long(res->get(EManagerFlds::IP))),
		_port(res->get<decltype(_port)>(EManagerFlds::PORT)),  
		_status(res->get<decltype(_status)>(EManagerFlds::STATUS))
{
}

bool ClusterManager::_loadManagers(Mysql &sql)
{
	auto res = sql.query(MANAGER_SQL);
	if (!res)	{
		log::Error::L("Cannot load information about manager nodes\n");
		return false;
	}
	while (res->next()) {
		ManagerNode *manager = new ManagerNode(res.get());
		_managers.insert(TManagerNodeMap::value_type(manager->id(), manager));
	}
	log::Info::L("Load %u managers\n", _managers.size());
	return true;
}

namespace EStorageFlds
{
	enum EStorageFlds
	{
		ID,
		GROUPID,
		IP,
		PORT,
		STATUS,
	};
};

const char * const  STORAGE_SQL = "SELECT id, groupID, ip, port, status FROM storage";

StorageNode::StorageNode(MysqlResult *res)
	: _id(res->get<decltype(_id)>(EStorageFlds::ID)), 
		_groupID(res->get<decltype(_groupID)>(EStorageFlds::GROUPID)), 
		_ip(Socket::ip2Long(res->get(EStorageFlds::IP))),
		_port(res->get<decltype(_port)>(EStorageFlds::PORT)),  
		_status(res->get<decltype(_status)>(EStorageFlds::STATUS))
{
	
}

bool ClusterManager::_loadStorages(Mysql &sql)
{
	auto res = sql.query(STORAGE_SQL);
	if (!res)	{
		log::Error::L("Cannot load information about storage nodes\n");
		return false;
	}
	while (res->next()) {
		StorageNode *storage = new StorageNode(res.get());
		_storages.insert(TStorageNodeMap::value_type(storage->id(), storage));
	}
	log::Info::L("Load %u storages\n", _storages.size());
	return true;
}

bool ClusterManager::loadAll(Mysql &sql)
{
	if (!_loadManagers(sql))
		return false;

	if (!_loadStorages(sql))
		return false;

	return true;
}


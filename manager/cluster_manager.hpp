#pragma once
#ifndef __FL_MANAGER_CLUSTER_MANAGER_HPP
#define	__FL_MANAGER_CLUSTER_MANAGER_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis cluster control class implementation
///////////////////////////////////////////////////////////////////////////////

#include "../types.hpp"
#include "socket.hpp"
#include "config.hpp"
#include "mysql.hpp"
#include <map>

namespace fl {
	namespace metis {
		using fl::network::TIPv4;
		using fl::db::MysqlResult;
		
		class ManagerNode
		{
		public:
			ManagerNode(MysqlResult *res);
			const TServerID id() const
			{
				return _id;
			}
		private:
			TServerID _id;
			TIPv4 _ip;
			uint32_t _port;
			TManagerStatus _status;
		};
		typedef std::shared_ptr<ManagerNode> TManagerNodePtr;
		
		class StorageNode
		{
		public:
			StorageNode(MysqlResult *res);
			
			const TServerID id() const
			{
				return _id;
			}
		private:
			TServerID _id;
			TStorageGroupID _groupID;
			TIPv4 _ip;
			uint32_t _port;
			TStorageStatus _status;
		};
		typedef std::shared_ptr<StorageNode> TStorageNodePtr;
		
		class ClusterManager
		{
		public:
			bool loadAll(Mysql &sql);
		private:
			bool _loadManagers(Mysql &sql);
			bool _loadStorages(Mysql &sql);
			typedef std::map<TServerID, TManagerNodePtr> TManagerNodeMap;
			TManagerNodeMap _managers;
			
			typedef std::map<TServerID, TStorageNodePtr> TStorageNodeMap;
			TStorageNodeMap _storages;
		};
	};
};

#endif	// __FL_MANAGER_CLUSTER_MANAGER_HPP

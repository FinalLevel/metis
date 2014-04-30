#pragma once
#ifndef __FL_MANAGER_MANAGER_HPP
#define	__FL_MANAGER_MANAGER_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Manager server control class
///////////////////////////////////////////////////////////////////////////////

#include "cluster_manager.hpp"
#include "index.hpp"

namespace fl {
	namespace metis {
		class Manager
		{
		public:
			Manager(class Config *config);
			bool loadAll();
			IndexManager &index()
			{
				return _indexManager;
			}
			bool fillAndAdd(ItemHeader &item, TRangePtr &range, bool &wasAdded);
			bool findAndFill(ItemHeader &item, TRangePtr &range);
			bool addLevel(const TLevel level, const TSubLevel subLevel);
			StorageNode *getPutStorage(const TRangeID rangeID, const TSize size);
		private:
			class Config *_config;
			ClusterManager _clusterManager;
			IndexManager _indexManager;
		};
	};
};

#endif	// __FL_MANAGER_MANAGER_HPP

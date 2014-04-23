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
			Index &index()
			{
				return _index;
			}
			bool fillAndAdd(ItemHeader &item);
			bool addLevel(const TLevel level, const TSubLevel subLevel);
		private:
			class Config *_config;
			ClusterManager _clusterManager;
			Index _index;
		};
	};
};

#endif	// __FL_MANAGER_MANAGER_HPP

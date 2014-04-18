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
			bool loadAll(Config *config);
		private:
			ClusterManager _clusterManager;
			Index _index;
		};
	};
};

#endif	// __FL_MANAGER_MANAGER_HPP

#pragma once
#ifndef __FL_METIS_MANAGER_CMD_EVENT_HPP
#define	__FL_METIS_MANAGER_CMD_EVENT_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis manager command event system classes
///////////////////////////////////////////////////////////////////////////////

#include "web.hpp"

namespace fl {
	namespace metis {
		using namespace fl::events;
		
		class ManagerCmdThreadSpecificData : public HttpThreadSpecificData
		{
		public:
			ManagerCmdThreadSpecificData(class Config *config);
			virtual ~ManagerCmdThreadSpecificData() 
			{
			}
			class Config *config;
		};
		
		class ManagerCmdThreadSpecificDataFactory : public ThreadSpecificDataFactory
		{
		public:
			ManagerCmdThreadSpecificDataFactory(class Config *config);
			virtual ThreadSpecificData *create();
			virtual ~ManagerCmdThreadSpecificDataFactory() {};
		private:
			class Config *_config;
		};		
	};
};

#endif	// __FL_METIS_MANAGER_CMD_EVENT_HPP

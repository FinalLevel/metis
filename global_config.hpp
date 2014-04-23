#pragma once
#ifndef __FL_METIS_GLOBAL_CONFIG_HPP
#define	__FL_METIS_GLOBAL_CONFIG_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Global configuration class
///////////////////////////////////////////////////////////////////////////////

#include <boost/property_tree/ini_parser.hpp>
#include "mysql.hpp"
#include "../types.hpp"

namespace fl {
	namespace metis {
		using fl::db::Mysql;
		const char * const DEFAULT_CONFIG = SYSCONFDIR "/metis.cnf";
		
		const int DEFAULT_SOCKET_TIMEOUT = 60;
		const size_t DEFAULT_SOCKET_QUEUE_LENGTH = 10000;
		const size_t EPOLL_WORKER_STACK_SIZE = 100000;
		const size_t DEFAULT_WORKERS_COUNT = 2;
		
		const size_t DEFAULT_BUFFER_SIZE = 32000;
		const size_t DEFAULT_MAX_FREE_BUFFERS = 500;

		class GlobalConfig
		{
		public:
			GlobalConfig(int argc, char *argv[]);
			bool connectDb(Mysql &sql);
			void setProcessUserAndGroup();
		protected:
			void _parseUserGroupParams(boost::property_tree::ptree &pt, const char *level);
			std::string _configFileName;
			boost::property_tree::ptree _pt;
		private:
			void _parseDBParams(boost::property_tree::ptree &pt);

			std::string _userName;
			uint32_t _uid;
			std::string _groupName;
			uint32_t _gid;

			std::string _dbHost;
			std::string _dbUser;
			std::string _dbPassword;
			std::string _dbName;
			uint16_t _dbPort;
		};
	};
};

#endif	// __FL_METIS_GLOBAL_CONFIG_HPP

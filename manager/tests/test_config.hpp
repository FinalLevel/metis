#pragma once
#ifndef __FL_MANAGER_TEST_CONFIG_HPP
#define	__FL_MANAGER_TEST_CONFIG_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Manager's test configuration class
///////////////////////////////////////////////////////////////////////////////

#include <memory>
#include "config.hpp"
#include "mysql.hpp"

namespace fl {
	namespace metis {
		using namespace fl::db;
		
		const char * const DB_INSTALL_PATH = "../db/install.sql";
		
		class TestConfig
		{
		public:
			TestConfig();
			~TestConfig();
			Config *config()
			{
				return _config.get();
			}
		private:
			static const int MAX_ARGV = 5;
			static const char *ARGV[MAX_ARGV + 1];
			std::unique_ptr<Config> _config;
			
			bool _generateTestBase(const std::string &dbName, Mysql &sql);
		};
	};
};

#endif	// __FL_MANAGER_TEST_CONFIG_HPP

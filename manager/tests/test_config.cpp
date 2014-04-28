///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Manager's test configuration class
///////////////////////////////////////////////////////////////////////////////


#include "test_config.hpp"
#include "bstring.hpp"
#include "metis_log.hpp"
#include "file.hpp"

using namespace fl::metis;
using namespace fl::strings;
using namespace fl::fs;

const char *TestConfig::ARGV[MAX_ARGV + 1] = {
	"manager", "-s", "1", "-c", "./tests/manager_test.cnf", NULL
};

TestConfig::TestConfig()
{
	GlobalConfig globalConfig(MAX_ARGV, const_cast<char**>(ARGV));
	{
		Mysql sql;
		if (!globalConfig.connectDb(sql)) {
			log::Error::L("Can't connect to generate test base\n");
			throw std::exception();
		}
		if (!_generateTestBase(globalConfig.dbName(), sql)) {
			log::Error::L("Can't generate test base\n");
			throw std::exception();
		}
	}	
	_config.reset(new Config(MAX_ARGV, const_cast<char**>(ARGV)));
}

TestConfig::~TestConfig()
{
}

bool TestConfig::_generateTestBase(const std::string &dbName, Mysql &sql)
{
	auto sqlQuery = sql.createQuery();
	sqlQuery << "DROP DATABASE IF EXISTS " << dbName;
	if (!sql.execute(sqlQuery))
		return false;
	
	sqlQuery << CLR << "CREATE DATABASE " << dbName;
	if (!sql.execute(sqlQuery))
		return false;
	
	sqlQuery << CLR << "USE " << dbName;
	if (!sql.execute(sqlQuery))
		return false;
	if (!sql.setServerOption(MYSQL_OPTION_MULTI_STATEMENTS_ON)) {
		return false;
	}
	
	sqlQuery.clear();
	File fd;
	if (!fd.open(DB_INSTALL_PATH, O_RDONLY)) {	
		log::Error::L("Can't open %s\n", DB_INSTALL_PATH);
		return false;
	}
	auto fileSize = fd.fileSize();
	if (fd.read(sqlQuery.reserveBuffer(fileSize), fileSize) != fileSize) {
		log::Error::L("Can't read %s\n", DB_INSTALL_PATH);
		return false;
	}
	sqlQuery << "INSERT INTO manager SET id=1,cmdIp='127.0.0.1',cmdPort=12001,webDavIp='127.0.0.1',\
webDavPort=12002,webIp='127.0.0.1',webPort=12003, status=1;\n";

	sqlQuery << "INSERT INTO manager SET id=2,cmdIp='127.0.0.1',cmdPort=12004,webDavIp='127.0.0.1',\
webDavPort=12005,webIp='127.0.0.1',webPort=12006, status=1;\n";

	sqlQuery << "INSERT INTO storage_group SET id=1,name='virtual1';\n";
	sqlQuery << "INSERT INTO storage_group SET id=2,name='virtual2';\n";
	sqlQuery << "INSERT INTO storage_group SET id=3,name='virtual3';\n";
	
	sqlQuery << "INSERT INTO storage SET id=1,groupID=1,ip='127.0.0.1',port=12007,status=1;\n";
	sqlQuery << "INSERT INTO storage SET id=2,groupID=2,ip='127.0.0.1',port=12008,status=1;\n";
	sqlQuery << "INSERT INTO storage SET id=3,groupID=3,ip='127.0.0.1',port=12009,status=1;\n";

	if (!sql.execute(sqlQuery))
		return false;
	
	while (sql.nextResult()) {
	}

	return true;
}


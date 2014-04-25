///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Global configuration class implementation
///////////////////////////////////////////////////////////////////////////////

#include "global_config.hpp"
#include "bstring.hpp"
#include <grp.h>
#include <pwd.h>

using namespace fl::metis;
using fl::strings::BString;
using namespace boost::property_tree::ini_parser;

GlobalConfig::GlobalConfig(int argc, char *argv[])
	: _configFileName(DEFAULT_CONFIG), _uid(0), _gid(0), _dbPort(0)
{
	char ch;
	optind = 1;
	while ((ch = getopt(argc, argv, "c:s:d:m:")) != -1) {
		switch (ch) {
			case 'c':
				_configFileName = optarg;
			break;
		}
	}
	try
	{
		read_ini(_configFileName.c_str(), _pt);
	
		_parseDBParams(_pt);
		_parseUserGroupParams(_pt, "metis");
	}
	catch (ini_parser_error &err)
	{
		printf("Caught error %s when parse %s at line %lu\n", err.message().c_str(), err.filename().c_str(), err.line());
		throw err;
	}
	catch(...)
	{
		printf("Caught unknown exception while parsing ini file %s\n", _configFileName.c_str());
		throw;
	}
}


void GlobalConfig::_parseUserGroupParams(boost::property_tree::ptree &pt, const char *level)
{
	BString buf;
	buf << level << ".user";
	_userName = pt.get<decltype(_userName)>(buf.c_str(), "nobody");
	auto passwd = getpwnam(_userName.c_str());
	if (passwd) {
		_uid = passwd->pw_uid;
	} else {
		printf("User %s has not been found\n", _userName.c_str());
		throw std::exception();
	}
	buf.clear();
	buf << level << ".group";
	_groupName = pt.get<decltype(_groupName)>(buf.c_str(), "nobody");
	auto groupData = getgrnam(_groupName.c_str());
	if (groupData) {
		_gid = groupData->gr_gid;
	} else {
		printf("Group %s has not been found\n", _groupName.c_str());
		throw std::exception();
	}
}

void GlobalConfig::setProcessUserAndGroup()
{
	if (setgid(_gid) || setuid(_uid)) {
		log::Error::L("Cannot set the process user %s (%d) and gid %s (%d)\n", _userName.c_str(), _uid, _groupName.c_str(),
			_gid);
	} else {
		log::Error::L("Set the process user %s (%d) and gid %s (%d)\n", _userName.c_str(), _uid, _groupName.c_str(), _gid);
	}
}


void GlobalConfig::_parseDBParams(boost::property_tree::ptree &pt)
{
	_dbHost = pt.get<decltype(_dbHost)>("metis.dbHost", "");
	if (_dbHost.empty()) {
		printf("metis-storage.dbHost is not set\n");
		throw std::exception();
	}
	_dbUser = pt.get<decltype(_dbUser)>("metis.dbUser", "");
	if (_dbUser.empty()) {
		printf("metis-storage.dbUser is not set\n");
		throw std::exception();
	}
	_dbPassword = pt.get<decltype(_dbPassword)>("metis.dbPassword", "");
	_dbName = pt.get<decltype(_dbName)>("metis.dbName", "metis");
	_dbPort = pt.get<decltype(_dbPort)>("metis.dbPort", 0);
}

bool GlobalConfig::connectDb(Mysql &sql)
{
	return sql.connect(_dbHost.c_str(), _dbUser.c_str(), _dbPassword.c_str(), _dbName.c_str(), _dbPort);
}


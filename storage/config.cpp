///////////////////////////////////////////////////////////////////////////////
//
// Copyright Denys Misko <gdraal@gmail.com>, Final Level, 2014.
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis server's storage configuration class
///////////////////////////////////////////////////////////////////////////////

#include <boost/property_tree/ini_parser.hpp>
#include <grp.h>
#include <pwd.h>

#include "config.hpp"
#include "log.hpp"
#include "metis_log.hpp"

using namespace fl::metis;
using namespace boost::property_tree::ini_parser;

Config::Config(int argc, char *argv[])
	: _uid(0), _gid(0), _serverID(0), _status(0), _logLevel(FL_LOG_LEVEL), _dbPort(0), _cmdTimeout(0), 
		_workerQueueLength(0), _workers(0),	_bufferSize(0), _maxFreeBuffers(0), _port(0), _storageStatus(0), 
		_minDiskFree(0), _maxSliceSize(0)
{
	std::string configFileName(DEFAULT_CONFIG);
	double minDiskFree = DEFAULT_MIN_DISK_FREE;
	char ch;
	while ((ch = getopt(argc, argv, "c:s:d:m:")) != -1) {
		switch (ch) {
			case 'c':
				configFileName = optarg;
			break;
			case 's':
				_serverID = atoi(optarg);
			break;
			case 'd':
				_dataPath = optarg;
			break;
			case 'm':
				minDiskFree = atof(optarg);
			break;
		}
	}
	if (!_serverID) {
		printf("serverID is required\n");
		_usage();
		throw std::exception();
	}
	if (_dataPath.empty()) {
		printf("data path is required\n");
		_usage();
		throw std::exception();
	}
	
	boost::property_tree::ptree pt;
	try
	{
		read_ini(configFileName.c_str(), pt);
		_logPath = pt.get<decltype(_logPath)>("metis-storage.log", _logPath);
		_logLevel = pt.get<decltype(_logLevel)>("metis-storage.logLevel", _logLevel);
		if (pt.get<std::string>("metis-storage.logStdout", "on") == "on")
			_status |= ST_LOG_STDOUT;
		
		_parseDBParams(pt);
		_parseUserGroupParams(pt);
		_loadFromDB();
		
		_cmdTimeout =  pt.get<decltype(_cmdTimeout)>("metis-storage.cmdTimeout", DEFAULT_SOCKET_TIMEOUT);
		_workerQueueLength = pt.get<decltype(_workerQueueLength)>("metis-storage.socketQueueLength", 
			DEFAULT_SOCKET_QUEUE_LENGTH);
		_workers = pt.get<decltype(_workers)>("metis-storage.workers", DEFAULT_WORKERS_COUNT);
		
		_bufferSize = pt.get<decltype(_bufferSize)>("metis-storage.bufferSize", DEFAULT_BUFFER_SIZE);
		_maxFreeBuffers = pt.get<decltype(_maxFreeBuffers)>("metis-storage.maxFreeBuffers", DEFAULT_MAX_FREE_BUFFERS);
		
		_minDiskFree = pt.get<decltype(_minDiskFree)>("metis-storage.minDiskFree", minDiskFree);
		_maxSliceSize = pt.get<decltype(_maxSliceSize)>("metis-storage.maxSliceSize", DEFAULT_MAX_SLICE_SIZE);
	}
	catch (ini_parser_error &err)
	{
		printf("Caught error %s when parse %s at line %lu\n", err.message().c_str(), err.filename().c_str(), err.line());
		throw err;
	}
	catch(...)
	{
		printf("Caught unknown exception while parsing ini file %s\n", configFileName.c_str());
		throw;
	}
}

void Config::_usage()
{
	printf("usage: metis_storage -s serverID -d dataPath [-c configPath] [-m minDiskFree]\n");
}

void Config::_loadFromDB()
{
	Mysql sql;
	if (!connectDb(sql)) {
		printf("Cannot connect to db, check db parameters\n");
		throw std::exception();
	}
	enum EServerFlds
	{
		FLD_IP,
		FLD_PORT,
		FLD_STATUS
	};
	auto res = sql.query(sql.createQuery() << "SELECT ip, port, status FROM storage WHERE id=" <<= _serverID);
	if (!res || !res->next())	{
		printf("Cannot get information about %u storage\n", _serverID);
		throw std::exception();
	}
	_listenIp = res->get<decltype(_listenIp)>(FLD_IP);
	_port = res->get<decltype(_port)>(FLD_PORT);
	_storageStatus = res->get<decltype(_storageStatus)>(FLD_STATUS);
	if (!(_storageStatus & ST_STORAGE_ACTIVE)) {
		printf("Cannot load an inactive storage %u\n", _serverID);
		throw std::exception();
	}
}


void Config::_parseUserGroupParams(boost::property_tree::ptree &pt)
{
	_userName = pt.get<decltype(_userName)>("metis-storage.user", "nobody");
	auto passwd = getpwnam(_userName.c_str());
	if (passwd) {
		_uid = passwd->pw_uid;
	} else {
		printf("User %s has not been found\n", _userName.c_str());
		throw std::exception();
	}

	_groupName = pt.get<decltype(_groupName)>("metis-storage.group", "nobody");
	auto groupData = getgrnam(_groupName.c_str());
	if (groupData) {
		_gid = groupData->gr_gid;
	} else {
		printf("Group %s has not been found\n", _groupName.c_str());
		throw std::exception();
	}
}

void Config::setProcessUserAndGroup()
{
	if (setgid(_gid) || setuid(_uid)) {
		log::Error::L("Cannot set the process user %s (%d) and gid %s (%d)\n", _userName.c_str(), _uid, _groupName.c_str(),
			_gid);
	} else {
		log::Error::L("Set the process user %s (%d) and gid %s (%d)\n", _userName.c_str(), _uid, _groupName.c_str(), _gid);
	}
}


void Config::_parseDBParams(boost::property_tree::ptree &pt)
{
	_dbHost = pt.get<decltype(_dbHost)>("metis-storage.dbHost", "");
	if (_dbHost.empty()) {
		printf("metis-storage.dbHost is not set\n");
		throw std::exception();
	}
	_dbUser = pt.get<decltype(_dbUser)>("metis-storage.dbUser", "");
	if (_dbUser.empty()) {
		printf("metis-storage.dbUser is not set\n");
		throw std::exception();
	}
	_dbPassword = pt.get<decltype(_dbPassword)>("metis-storage.dbPassword", "");
	_dbName = pt.get<decltype(_dbName)>("metis-storage.dbName", "metis");
	_dbPort = pt.get<decltype(_dbPort)>("metis-storage.dbPort", 0);
}

bool Config::connectDb(Mysql &sql)
{
	return sql.connect(_dbHost.c_str(), _dbUser.c_str(), _dbPassword.c_str(), _dbName.c_str(), _dbPort);
}

bool Config::initNetwork()
{
	if (!_listenSocket.listen(_listenIp.c_str(), _port))	{
		log::Error::L("Metis storage %u cannot begin listening on %s:%u\n", _serverID, _listenIp.c_str(), _port);
		return false;
	}
	log::Warning::L("Metis storage %u is listening on %s:%u\n", _serverID, _listenIp.c_str(), _port);
	return true;
}

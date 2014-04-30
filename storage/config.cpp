///////////////////////////////////////////////////////////////////////////////
//
// Copyright Denys Misko <gdraal@gmail.com>, Final Level, 2014.
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis server's storage configuration class
///////////////////////////////////////////////////////////////////////////////

#include <boost/property_tree/ini_parser.hpp>

#include "config.hpp"
#include "log.hpp"
#include "metis_log.hpp"

using namespace fl::metis;
using namespace boost::property_tree::ini_parser;
using fl::db::ESC;

Config::Config(int argc, char *argv[])
	: GlobalConfig(argc, argv), _serverID(0), _status(0), _logLevel(FL_LOG_LEVEL), _cmdTimeout(0), 
		_workerQueueLength(0), _workers(0),	_bufferSize(0), _maxFreeBuffers(0), _port(0), _storageStatus(0), 
		_minDiskFree(0), _maxSliceSize(0)
{
	double minDiskFree = DEFAULT_MIN_DISK_FREE;
	char ch;
	optind = 1;
	while ((ch = getopt(argc, argv, "s:d:m:")) != -1) {
		switch (ch) {
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

	try
	{
		_logPath = _pt.get<decltype(_logPath)>("metis-storage.log", _logPath);
		_logLevel = _pt.get<decltype(_logLevel)>("metis-storage.logLevel", _logLevel);
		if (_pt.get<std::string>("metis-storage.logStdout", "on") == "on")
			_status |= ST_LOG_STDOUT;
		
		_parseUserGroupParams(_pt, "metis-storage");
		_loadFromDB();
		
		_cmdTimeout =  _pt.get<decltype(_cmdTimeout)>("metis-storage.cmdTimeout", DEFAULT_SOCKET_TIMEOUT);
		_workerQueueLength = _pt.get<decltype(_workerQueueLength)>("metis-storage.socketQueueLength", 
			DEFAULT_SOCKET_QUEUE_LENGTH);
		_workers = _pt.get<decltype(_workers)>("metis-storage.workers", DEFAULT_WORKERS_COUNT);
		
		_bufferSize = _pt.get<decltype(_bufferSize)>("metis-storage.bufferSize", DEFAULT_BUFFER_SIZE);
		_maxFreeBuffers = _pt.get<decltype(_maxFreeBuffers)>("metis-storage.maxFreeBuffers", DEFAULT_MAX_FREE_BUFFERS);
		
		_minDiskFree = _pt.get<decltype(_minDiskFree)>("metis-storage.minDiskFree", minDiskFree);
		_maxSliceSize = _pt.get<decltype(_maxSliceSize)>("metis-storage.maxSliceSize", DEFAULT_MAX_SLICE_SIZE);
		
		_tmpDir = _pt.get<decltype(_tmpDir)>("metis-storage.tmpDir", "/tmp");
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
	auto res = sql.query(sql.createQuery() << "SELECT ip, port, status FROM storage WHERE id=" << ESC << _serverID);
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

bool Config::initNetwork()
{
	if (!_listenSocket.listen(_listenIp.c_str(), _port))	{
		log::Error::L("Metis storage %u cannot begin listening on %s:%u\n", _serverID, _listenIp.c_str(), _port);
		return false;
	}
	log::Warning::L("Metis storage %u is listening on %s:%u\n", _serverID, _listenIp.c_str(), _port);
	return true;
}

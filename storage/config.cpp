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

Config::Config(int argc, char *argv[])
	: _serverID(0), _status(0), _logLevel(FL_LOG_LEVEL), _cmdTimeout(0), _workerQueueLength(0), _workers(0),	
		_bufferSize(0), _maxFreeBuffers(0), _port(0)
{
	std::string configFileName(DEFAULT_CONFIG);
	char ch;
	while ((ch = getopt(argc, argv, "c:")) != -1) {
		switch (ch) {
			case 'c':
				configFileName = optarg;
			break;
		}
	}
	
	boost::property_tree::ptree pt;
	try
	{
		read_ini(configFileName.c_str(), pt);
		_logPath = pt.get<decltype(_logPath)>("metis-server.log", _logPath);
		_logLevel = pt.get<decltype(_logLevel)>("metis-server.logLevel", _logLevel);
		if (pt.get<std::string>("metis-server.logStdout", "on") == "on")
			_status |= ST_LOG_STDOUT;
		
		_serverID = pt.get<decltype(_serverID)>("metis-storage.serverID", 0);
		if (_serverID == 0) {
			printf("metis-storage.serverID cannot be zero\n");
			throw std::exception();
		}
		_dataPath = pt.get<decltype(_dataPath)>("metis-storage.dataPath", "");
		if (_dataPath.empty()) {
			printf("metis-storage.dataPath is not set\n");
			throw std::exception();
		}
			
		
		_cmdTimeout =  pt.get<decltype(_cmdTimeout)>("metis-storage.cmdTimeout", DEFAULT_SOCKET_TIMEOUT);
		_workerQueueLength = pt.get<decltype(_workerQueueLength)>("metis-storage.socketQueueLength", 
			DEFAULT_SOCKET_QUEUE_LENGTH);
		_workers = pt.get<decltype(_workers)>("metis-storage.workers", DEFAULT_WORKERS_COUNT);
		
		_bufferSize = pt.get<decltype(_bufferSize)>("metis-storage.bufferSize", DEFAULT_BUFFER_SIZE);
		_maxFreeBuffers = pt.get<decltype(_maxFreeBuffers)>("metis-storage.maxFreeBuffers", DEFAULT_MAX_FREE_BUFFERS);
		
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

void Config::_parseNetworkParams(boost::property_tree::ptree &pt)
{
	_listenIp = pt.get<decltype(_listenIp)>("metis-storage.listen", "");
	if (_listenIp.empty()) {
		printf("nomos-server.listenIP is not set\n");
		throw std::exception();
	}
	_port = pt.get<decltype(_port)>("metis-storage.port", DEFAULT_CMD_PORT);
	
}

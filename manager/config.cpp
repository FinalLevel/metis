///////////////////////////////////////////////////////////////////////////////
//
// Copyright Denys Misko <gdraal@gmail.com>, Final Level, 2014.
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis server's config class
///////////////////////////////////////////////////////////////////////////////

#include <boost/property_tree/ini_parser.hpp>

#include "config.hpp"
#include "log.hpp"
#include "metis_log.hpp"

using namespace fl::metis;
using namespace boost::property_tree::ini_parser;

Config::Config(int argc, char *argv[])
	: GlobalConfig(argc, argv), _serverID(0), _status(0), _logLevel(FL_LOG_LEVEL), _port(0)
{
	char ch;
	optind = 1;
	while ((ch = getopt(argc, argv, "s:")) != -1) {
		switch (ch) {
			case 's':
				_serverID = atoi(optarg);
			break;
		}
	}
	if (!_serverID) {
		printf("serverID is required\n");
		_usage();
		throw std::exception();
	}

	try
	{
		_logPath = _pt.get<decltype(_logPath)>("metis-manager.log", _logPath);
		_logLevel = _pt.get<decltype(_logLevel)>("metis-manager.logLevel", _logLevel);
		if (_pt.get<std::string>("metis-manager.logStdout", "on") == "on")
			_status |= ST_LOG_STDOUT;

		_parseUserGroupParams(_pt, "metis-manager");
		_loadFromDB();
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
	printf("usage: metis_manager -s serverID [-c configPath]\n");
}

void Config::_loadFromDB()
{
	Mysql sql;
	if (!connectDb(sql)) {
		printf("Cannot connect to db, check db parameters\n");
		throw std::exception();
	}
	enum EManagerFlds
	{
		FLD_IP,
		FLD_PORT,
	};
	auto res = sql.query(sql.createQuery() << "SELECT ip, port FROM manager WHERE id=" <<= _serverID);
	if (!res || !res->next())	{
		printf("Cannot get information about %u manager\n", _serverID);
		throw std::exception();
	}
	_listenIp = res->get<decltype(_listenIp)>(FLD_IP);
	_port = res->get<decltype(_port)>(FLD_PORT);
}

bool Config::initNetwork()
{
	if (!_listenSocket.listen(_listenIp.c_str(), _port))	{
		log::Error::L("Metis manager %u cannot begin listening on %s:%u\n", _serverID, _listenIp.c_str(), _port);
		return false;
	}
	log::Warning::L("Metis manager %u is listening on %s:%u\n", _serverID, _listenIp.c_str(), _port);
	return true;
}




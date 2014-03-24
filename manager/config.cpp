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
	: _status(0), _logLevel(FL_LOG_LEVEL)
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

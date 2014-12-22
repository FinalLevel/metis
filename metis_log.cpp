///////////////////////////////////////////////////////////////////////////////
//
// Copyright Denys Misko <gdraal@gmail.com>, Final Level, 2014.
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis log system customization class
///////////////////////////////////////////////////////////////////////////////

#include "metis_log.hpp"
using namespace fl::metis::log;

int MetisLogSystem::_logLevel { FL_LOG_LEVEL};


bool MetisLogSystem::log(
	const size_t target, 
	const int level, 
	const time_t curTime, 
	struct tm *ct, 
	const char *fmt, 
	va_list args
)
{
	if (level <= _logLevel)
		return LogSystem::defaultLog().log(target, level, "MS", curTime, ct, fmt, args);
	else
		return false;
}


bool MetisLogSystem::init(const int logLevel, const std::string &logPath, const bool isLogStdout)
{
	LogSystem::defaultLog().clearTargets();
	_logLevel = logLevel;
	if (!logPath.empty())
		LogSystem::defaultLog().addTarget(new fl::log::FileTarget(logPath.c_str()));
	if (isLogStdout)
		LogSystem::defaultLog().addTarget(new fl::log::ScreenTarget());
	return true;
}

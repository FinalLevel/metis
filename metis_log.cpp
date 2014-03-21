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

MetisLogSystem MetisLogSystem::_logSystem;

MetisLogSystem::MetisLogSystem()
	: LogSystem("metis"), _logLevel(MAX_LOG_LEVEL)
{
	_logSystem.addTarget(new fl::log::ScreenTarget());
}

bool MetisLogSystem::log(
	const size_t target, 
	const int level, 
	const time_t curTime, 
	struct tm *ct, 
	const char *fmt, 
	va_list args
)
{
	if (level <= _logSystem._logLevel)
		return _logSystem._log(target, level, curTime, ct, fmt, args);
	else
		return false;
}

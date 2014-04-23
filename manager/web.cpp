///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis manager http event system classes implementation
///////////////////////////////////////////////////////////////////////////////

#include "web.hpp"
#include "config.hpp"

using namespace fl::metis;

ManagerHttpInterface::ManagerHttpInterface()
{
}

ManagerHttpInterface::~ManagerHttpInterface()
{
}

bool ManagerHttpInterface::parseURI(const char *cmdStart, const EHttpVersion::EHttpVersion version, 
	const std::string &host, const std::string &fileName, const std::string &query)
{
	return false;
}

ManagerHttpInterface::EFormResult ManagerHttpInterface::formResult(BString &networkBuffer, class HttpEvent *http)
{
	return EFormResult::RESULT_ERROR;
}

ManagerEventFactory::ManagerEventFactory(Config *config)
	: _config(config)
{
}

WorkEvent *ManagerEventFactory::create(const TEventDescriptor descr, const TIPv4 ip, const time_t timeOutTime, 
	Socket *acceptSocket)
{
	return new HttpEvent(descr, EPollWorkerGroup::curTime.unix() + _config->webTimeout(), new ManagerHttpInterface());
}

ManagerHttpThreadSpecificDataFactory::ManagerHttpThreadSpecificDataFactory(class Config *config)
	: _config(config)
{
}

ThreadSpecificData *ManagerHttpThreadSpecificDataFactory::create()
{
	return new ManagerHttpThreadSpecificData(_config);
}

ManagerHttpThreadSpecificData::ManagerHttpThreadSpecificData(class Config *config)
	: config(config)
{
}


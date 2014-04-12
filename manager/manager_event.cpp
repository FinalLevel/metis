///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis event system classes implementation
///////////////////////////////////////////////////////////////////////////////

#include "manager_event.hpp"

using namespace fl::metis;

ManagerHttpInterface::ManagerHttpInterface()
{
}

ManagerHttpInterface::~ManagerHttpInterface()
{
}

bool ManagerHttpInterface::parseURI(const EHttpRequestType::EHttpRequestType reqType, 
	const EHttpVersion::EHttpVersion version, const std::string &host, const std::string &fileName, 
	const std::string &query)
{
	return false;
}

ManagerHttpInterface::EFormResult ManagerHttpInterface::formResult(BString &networkBuffer, class HttpEvent *http)
{
	return EFormResult::RESULT_ERROR;
}

			
WorkEvent *create(const TEventDescriptor descr, const TIPv4 ip, const time_t timeOutTime, Socket *acceptSocket)
{
	return new HttpEvent(descr, timeOutTime, new ManagerHttpInterface());
}

ManagerThreadSpecificDataFactory::ManagerThreadSpecificDataFactory()
{
}

ThreadSpecificData *ManagerThreadSpecificDataFactory::create()
{
	return new ManagerHttpThreadSpecificData();
}


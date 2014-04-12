#pragma once
#ifndef __FL_METIS_EVENT_HPP
#define	__FL_METIS_EVENT_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis event system classes
///////////////////////////////////////////////////////////////////////////////

#include "http_event.hpp"

namespace fl {
	namespace metis {
		using namespace fl::events;
		
		class ManagerHttpInterface : public HttpEventInterface
		{
		public:
			ManagerHttpInterface();
			virtual bool parseURI(const EHttpRequestType::EHttpRequestType reqType, const EHttpVersion::EHttpVersion version,
				const std::string &host, const std::string &fileName, const std::string &query);
			virtual EFormResult formResult(BString &networkBuffer, class HttpEvent *http);
			virtual ~ManagerHttpInterface();
		private:
			
		};
	
		class ManagerEventFactory : public WorkEventFactory 
		{
		public:
			ManagerEventFactory()
			{
			}
			virtual WorkEvent *create(const TEventDescriptor descr, const TIPv4 ip, const time_t timeOutTime, 
				Socket *acceptSocket);
			virtual ~ManagerEventFactory() {};
		};

		class ManagerHttpThreadSpecificData : public HttpThreadSpecificData
		{
		public:
			virtual ~ManagerHttpThreadSpecificData() 
			{
			}
		};
		
		class ManagerThreadSpecificDataFactory : public ThreadSpecificDataFactory
		{
		public:
			ManagerThreadSpecificDataFactory();
			virtual ThreadSpecificData *create();
			virtual ~ManagerThreadSpecificDataFactory() {};
		};

	};
};

#endif	// __FL_METIS_EVENT_HPP

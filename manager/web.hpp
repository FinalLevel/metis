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
// Description: Metis manager http event system classes
///////////////////////////////////////////////////////////////////////////////

#include "http_event.hpp"

namespace fl {
	namespace metis {
		using namespace fl::events;
		
		class ManagerHttpInterface : public HttpEventInterface
		{
		public:
			ManagerHttpInterface();
			virtual bool parseURI(const char *cmdStart, const EHttpVersion::EHttpVersion version,
				const std::string &host, const std::string &fileName, const std::string &query);
			virtual EFormResult formResult(BString &networkBuffer, class HttpEvent *http);
			virtual ~ManagerHttpInterface();
		private:
			
		};
	
		class ManagerEventFactory : public WorkEventFactory 
		{
		public:
			ManagerEventFactory(class Config *config);
			virtual WorkEvent *create(const TEventDescriptor descr, const TIPv4 ip, const time_t timeOutTime, 
				Socket *acceptSocket);
			virtual ~ManagerEventFactory() {};
		private:
			class Config *_config;
		};

		class ManagerHttpThreadSpecificData : public HttpThreadSpecificData
		{
		public:
			ManagerHttpThreadSpecificData(class Config *config);
			virtual ~ManagerHttpThreadSpecificData() 
			{
			}
			class Config *config;
		};
		
		class ManagerHttpThreadSpecificDataFactory : public ThreadSpecificDataFactory
		{
		public:
			ManagerHttpThreadSpecificDataFactory(class Config *config);
			virtual ThreadSpecificData *create();
			virtual ~ManagerHttpThreadSpecificDataFactory() {};
		private:
			class Config *_config;
		};

	};
};

#endif	// __FL_METIS_EVENT_HPP

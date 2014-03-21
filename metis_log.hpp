#pragma once
#ifndef __FL_METIS_LOG_HPP
#define	__FL_METIS_LOG_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright Denys Misko <gdraal@gmail.com>, Final Level, 2014.
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis log system customization class
///////////////////////////////////////////////////////////////////////////////

#include "log.hpp"
#include "config.hpp"

namespace fl {
	namespace metis {
		namespace log {
			
			using fl::log::Target;
			using fl::log::TTargetList;
			using fl::log::LogSystem;

			class MetisLogSystem : public LogSystem
			{
			public:
				MetisLogSystem();
				static bool log(
					const size_t target, 
					const int level, 
					const time_t curTime, 
					struct tm *ct, 
					const char *fmt, 
					va_list args
				);
				//static bool init(fl::nomos::Config *config);
			private:
				static MetisLogSystem _logSystem;
				int _logLevel;
			};
		
			using fl::log::Log;
			using namespace fl::log::ELogLevel;

			typedef Log<true, ELogLevel::INFO, MetisLogSystem> Info;
			typedef Log<true, ELogLevel::WARNING, MetisLogSystem> Warning;
			typedef Log<true, ELogLevel::ERROR, MetisLogSystem> Error;
			typedef Log<true, ELogLevel::FATAL, MetisLogSystem> Fatal;
		};
	};
};

#endif	// __FL_METIS_LOG_HPP

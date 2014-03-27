#pragma once
#ifndef __FL_METIS_MANAGER_CONFIG_HPP
#define	__FL_METIS_MANAGER_CONFIG_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright Denys Misko <gdraal@gmail.com>, Final Level, 2014.
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis server's config class
///////////////////////////////////////////////////////////////////////////////

#include <boost/property_tree/ptree.hpp>

#include <string>
#include <vector>
#include "socket.hpp"

namespace fl {
	namespace metis {
		using fl::network::Socket;

		const char * const DEFAULT_CONFIG = SYSCONFDIR "/metis.cnf";
		const size_t MAX_BUF_SIZE = 300000;
		class Config
		{
		public:
			Config(int argc, char *argv[]);
			const std::string &logPath() const
			{
				return _logPath;
			}
			int logLevel() const
			{
				return _logLevel;
			}
			typedef uint32_t TStatus;
			static const TStatus ST_LOG_STDOUT = 0x1;
			const bool isLogStdout() const
			{
				return _status & ST_LOG_STDOUT;
			}
		private:
			TStatus _status;
			std::string _logPath;
			int _logLevel;
		};
	}
}

#endif // __FL_METIS_MANAGER_CONFIG_HPP
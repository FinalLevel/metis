#pragma once
#ifndef __FL_METIS_MANAGER_CONFIG_HPP
#define	__FL_METIS_MANAGER_CONFIG_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright Denys Misko <gdraal@gmail.com>, Final Level, 2014.
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis manager's server configuration class
///////////////////////////////////////////////////////////////////////////////

#include <boost/property_tree/ptree.hpp>

#include <string>
#include <vector>
#include "../global_config.hpp"
#include "socket.hpp"

namespace fl {
	namespace metis {
		using fl::network::Socket;

		const size_t MAX_BUF_SIZE = 300000;
		const size_t DEFAULT_MINIMUM_COPIES = 2;
		const size_t DEFAULT_MAX_CONNECTION_PER_STORAGE = 2;
		
		class Config : public GlobalConfig
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
			const TServerID serverID() const
			{
				return _serverID;
			}
			bool initNetwork();
			Socket &cmdSocket()
			{
				return _cmdSocket;
			}
			Socket &webSocket()
			{
				return _webSocket;
			}
			Socket &webDavSocket()
			{
				return _webDavSocket;
			}
			
			const int cmdTimeout() const
			{
				return _cmdTimeout;
			}

			const int webTimeout() const
			{
				return _webTimeout;
			}
	
			const int webDavTimeout() const
			{
				return _webDavTimeout;
			}
			size_t webWorkerQueueLength() const
			{
				return _webWorkerQueueLength;
			}
			size_t webWorkers() const
			{
				return _webWorkers;
			}

			size_t cmdWorkerQueueLength() const
			{
				return _cmdWorkerQueueLength;
			}
			size_t cmdWorkers() const
			{
				return _cmdWorkers;
			}
			size_t minimumCopies() const
			{
				return _minimumCopies;
			}
			size_t maxConnectionPerStorage() const
			{
				return _maxConnectionPerStorage;
			}
		private:
			void _usage();
			void _loadFromDB();
			
			TServerID _serverID;
			TStatus _status;
			std::string _logPath;
			int _logLevel;
			
			std::string _cmdIp;
			uint32_t _cmdPort;
			Socket _cmdSocket;

			std::string _webDavIp;
			uint32_t _webDavPort;
			Socket _webDavSocket;

			std::string _webIp;
			uint32_t _webPort;
			Socket _webSocket;

			int _cmdTimeout;
			int _webTimeout;
			int _webDavTimeout;
			size_t _webWorkerQueueLength;
			size_t _webWorkers;

			size_t _cmdWorkerQueueLength;
			size_t _cmdWorkers;
				
			size_t _bufferSize;
			size_t _maxFreeBuffers;
			
			size_t _minimumCopies;
			size_t _maxConnectionPerStorage;
		};
	}
}

#endif // __FL_METIS_MANAGER_CONFIG_HPP
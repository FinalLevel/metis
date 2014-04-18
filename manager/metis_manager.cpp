///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis Storage is a CDN and high available http server, which is especially useful for storing 
// and delivering of a huge number of small and medium size objects (images, js files, etc) 
///////////////////////////////////////////////////////////////////////////////


#include <memory>
#include <signal.h>
#include "socket.hpp"
#include "config.hpp"
#include "metis_log.hpp"
#include "time.hpp"
#include "accept_thread.hpp"
#include "manager_event.hpp"
#include "manager.hpp"


using fl::network::Socket;
using fl::chrono::Time;
using namespace fl::metis;
using namespace fl::events;

void sigInt(int sig)
{
	log::Fatal::L("Interruption signal (%d) has been received - flushing data\n", sig);
	static fl::threads::Mutex sigSync;
	if (!sigSync.tryLock())
		return;
	
	exit(0);
}

void setSignals()
{
	signal(SIGINT, sigInt);
	signal(SIGTERM, sigInt);
}

int main(int argc, char *argv[])
{
	std::unique_ptr<Config> config;
	std::unique_ptr<EPollWorkerGroup> workerGroup;
	std::unique_ptr<Manager> manager;
	try
	{
		config.reset(new Config(argc, argv));
		if (!log::MetisLogSystem::init(config->logLevel(), config->logPath(), config->isLogStdout()))
			return -1;
		if (!config->initNetwork())
			return -1;
		config->setProcessUserAndGroup();

		log::Warning::L("Starting Metis Manager server %u\n", config->serverID());
		manager.reset(new Manager());
		if (!manager->loadAll(config.get()))
			return -1;
	}
	catch (...)	
	{
		return -1;
	}
	return 0;
};

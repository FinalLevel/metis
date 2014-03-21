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
#include "metis_event.hpp"


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
}

int main(int argc, char *argv[])
{
	
	return 0;
};

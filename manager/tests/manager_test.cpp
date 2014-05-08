///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis manager functional tests
///////////////////////////////////////////////////////////////////////////////

#include <boost/test/unit_test.hpp>
#include "test_path.hpp"
#include "manager.hpp"
#include "test_config.hpp"

using namespace fl::metis;
using fl::tests::TestPath;
using fl::fs::Directory;

BOOST_AUTO_TEST_SUITE( metis )

BOOST_AUTO_TEST_CASE (testLevelRangeCreation)
{
	try
	{
		TestConfig config;
		Manager manager(config.config());
		BOOST_REQUIRE(manager.loadAll());

		ItemHeader item;
		item.level = 1;
		item.subLevel = 1;
		item.itemKey = 1;

		BOOST_REQUIRE(manager.addLevel(item.level, item.subLevel));

		TRangePtr range;
		bool wasAdded;
		auto storages = manager.clusterManager().storages();
		for (auto s = storages.begin(); s != storages.end(); s++)
		{
			StoragePingAnswer storageAnswer;
			storageAnswer.serverID = (*s)->id();
			static const size_t DEFAULT_RANGE_SIZE = 320000;
			storageAnswer.leftSpace = config.config()->averageItemSize() * DEFAULT_RANGE_SIZE * 2;
			(*s)->ping(storageAnswer);
		}
		BOOST_REQUIRE(manager.fillAndAdd(item, range, wasAdded));
		BOOST_REQUIRE(range->rangeID() == 1);
		BOOST_REQUIRE(range->rangeIndex() == 0);
		BOOST_REQUIRE(wasAdded);
		
		item.itemKey = 2000000;
		BOOST_REQUIRE(manager.fillAndAdd(item, range, wasAdded));
		BOOST_REQUIRE(range->rangeID() == 2);
		BOOST_REQUIRE(range->rangeIndex() == 15);
		BOOST_REQUIRE(wasAdded);
		
		item.rangeID = 0;
		BOOST_REQUIRE(manager.findAndFill(item, range));
		BOOST_REQUIRE(item.rangeID == range->rangeID());
		
		Manager manager2(config.config());
	} catch (...) {
		BOOST_CHECK_NO_THROW(throw);
	}
}

BOOST_AUTO_TEST_CASE (testParseURLToItem)
{
	try
	{
		TestConfig config;
		Manager manager(config.config());
		BOOST_REQUIRE(manager.loadAll());

		ItemHeader item;
		TCrc urlCrc;
		BOOST_CHECK(manager.index().parseURL("", "/1/2/", item, urlCrc));
		BOOST_CHECK((item.level == 1) && (item.subLevel == 2));
		
	} catch (...) {
		BOOST_CHECK_NO_THROW(throw);
	}
}

BOOST_AUTO_TEST_SUITE_END()
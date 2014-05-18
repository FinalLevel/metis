///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Metis manager cache tests
///////////////////////////////////////////////////////////////////////////////

#include <boost/test/unit_test.hpp>
#include "cache.hpp"

using namespace fl::metis;

BOOST_AUTO_TEST_SUITE( metis )
				
BOOST_AUTO_TEST_CASE (testCacheCreation)
{
	try
	{	
		const size_t CACHE_SIZE = 10000;
		const size_t ITEM_HEADER_CASHE_SIZE = 1000;
		const TCacheLineIndex ITEMS_PER_LINE = 10;
		Cache cache(CACHE_SIZE, ITEM_HEADER_CASHE_SIZE, ITEMS_PER_LINE, 0);
		
		BOOST_CHECK(cache.countLines() == 2);
		BOOST_CHECK(cache.leftMem() == (int64_t)CACHE_SIZE);
	}
	catch (...) {
		BOOST_CHECK_NO_THROW(throw);
	}
}

BOOST_AUTO_TEST_CASE (testCacheAddAndFindAndRemove)
{
	try
	{	
		const size_t CACHE_SIZE = 10000;
		const size_t ITEM_HEADER_CASHE_SIZE = 1000;
		const TCacheLineIndex ITEMS_PER_LINE = 10;
		const size_t MIN_HITS_TO_CACHE = 1;
		Cache cache(CACHE_SIZE, ITEM_HEADER_CASHE_SIZE, ITEMS_PER_LINE, MIN_HITS_TO_CACHE);
		
		ItemInfo item;
		item.index.rangeID = 1;
		item.index.itemKey = 1;
		item.size = 9000;
		item.timeTag.tag = 1292902180280;
		
		TStorageList storages;
		storages.push_back(NULL);
		
		BOOST_REQUIRE(cache.replace(item, storages));
		
		BString testData;
		testData.reserveBuffer(item.size);

		BOOST_REQUIRE(cache.replaceData(item, testData.c_str())  == false);
		BOOST_CHECK(cache.leftMem() == (int64_t)CACHE_SIZE);
		
		BString buffer;
		TStorageList findStorages;
		ItemInfo findItem(item);
		findItem.size = 0;
		findItem.timeTag.tag = 0;
		BOOST_REQUIRE(cache.findAndFill(findItem, findStorages, buffer) 
			== ECacheFindResult::FIND_HEADER_ONLY);
		BOOST_REQUIRE(findItem.size == item.size);
		BOOST_REQUIRE(findItem.timeTag.tag == item.timeTag.tag);
		
		BOOST_REQUIRE(cache.replaceData(item, testData.c_str()));
		BOOST_CHECK(cache.leftMem() == (int64_t)(CACHE_SIZE - item.size));
		
		BOOST_REQUIRE(cache.findAndFill(findItem, findStorages, buffer) 
			== ECacheFindResult::FIND_FULL);
		BOOST_REQUIRE((TItemSize)buffer.size() == item.size);
		BOOST_REQUIRE(cache.replaceData(item, testData.c_str()) == false);
		
		storages.clear();
		BOOST_REQUIRE(cache.replace(item, storages) == false);
		BOOST_REQUIRE(cache.remove(item.index));
		BOOST_CHECK(cache.leftMem() == (int64_t)CACHE_SIZE);
		buffer.clear();
		BOOST_REQUIRE(cache.findAndFill(findItem, findStorages, buffer) 
			== ECacheFindResult::FIND_NOT_FOUND);
		
		item.index.itemKey++;
		findItem.index.itemKey = item.index.itemKey;
		BOOST_REQUIRE(cache.findAndFill(findItem, findStorages, buffer) 
			== ECacheFindResult::NOT_IN_CACHE);
		BOOST_REQUIRE(cache.remove(item.index));
		BOOST_REQUIRE(cache.findAndFill(findItem, findStorages, buffer) 
			== ECacheFindResult::FIND_NOT_FOUND);
		BOOST_REQUIRE(cache.replaceData(item, testData.c_str()) == false);
	}
	catch (...) {
		BOOST_CHECK_NO_THROW(throw);
	}	
}

BOOST_AUTO_TEST_CASE (testCacheRecycle)
{
	try
	{	
		const size_t CACHE_SIZE = 10000;
		const size_t ITEM_HEADER_CASHE_SIZE = 10000;
		const TCacheLineIndex ITEMS_PER_LINE = 300;
		const size_t MIN_HITS_TO_CACHE = 1;
		Cache cache(CACHE_SIZE, ITEM_HEADER_CASHE_SIZE, ITEMS_PER_LINE, MIN_HITS_TO_CACHE);
		
		ItemInfo item;
		item.index.rangeID = 1;
		item.index.itemKey = 1;
		item.size = 100;
		item.timeTag.tag = 1292902180280;
		
		TStorageList storages;
		storages.push_back(NULL);
		
		for (int i = 0; i < 100; i++) {
			item.index.itemKey = i + 1;
			BOOST_REQUIRE(cache.replace(item, storages));
		}


		BString buffer;

		for (int i = 0; i < 90; i++) {
			buffer.clear();
			item.index.itemKey = i + 1;
			TStorageList storages;
			BOOST_REQUIRE(cache.findAndFill(item, storages, buffer) 
				== ECacheFindResult::FIND_HEADER_ONLY);
		}
		for (int i = 15; i < 90; i++) {
			buffer.clear();
			item.index.itemKey = i + 1;
			TStorageList storages;
			BOOST_REQUIRE(cache.findAndFill(item, storages, buffer) 
				== ECacheFindResult::FIND_HEADER_ONLY);
		}
		BString testData;
		testData.reserveBuffer(item.size);
		for (int i = 0; i < 90; i++) {
			item.index.itemKey = i + 1;
			BOOST_REQUIRE(cache.replaceData(item, testData.c_str()));
		}
		
		cache.recycle();
		
		for (int i = 0; i < 15; i++) {
			buffer.clear();
			item.index.itemKey = i + 1;
			TStorageList storages;
			BOOST_REQUIRE(cache.findAndFill(item, storages, buffer) 
				== ECacheFindResult::NOT_IN_CACHE);
		}
	}
	catch (...) {
		BOOST_CHECK_NO_THROW(throw);
	}	
}

BOOST_AUTO_TEST_SUITE_END()
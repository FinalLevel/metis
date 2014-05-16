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
		
		GetItemInfoAnswer item;
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
		
		TItemSize size = 0;
		ModTimeTag modTimeTag;
		BString buffer;
		BOOST_REQUIRE(cache.find(item.index.rangeID, item.index.itemKey, size, modTimeTag, buffer) 
			== ECacheFindResult::FIND_HEADER_ONLY);
		BOOST_REQUIRE(size == item.size);
		BOOST_REQUIRE(modTimeTag.tag == item.timeTag.tag);
		
		BOOST_REQUIRE(cache.replaceData(item, testData.c_str()));
		BOOST_CHECK(cache.leftMem() == (int64_t)(CACHE_SIZE - item.size));
		
		BOOST_REQUIRE(cache.find(item.index.rangeID, item.index.itemKey, size, modTimeTag, buffer) 
			== ECacheFindResult::FIND_FULL);
		BOOST_REQUIRE((TItemSize)buffer.size() == item.size);
		BOOST_REQUIRE(cache.replaceData(item, testData.c_str()) == false);
		
		storages.clear();
		BOOST_REQUIRE(cache.replace(item, storages) == false);
		BOOST_REQUIRE(cache.remove(item.index));
		BOOST_CHECK(cache.leftMem() == (int64_t)CACHE_SIZE);
		BOOST_REQUIRE(cache.find(item.index.rangeID, item.index.itemKey, size, modTimeTag, buffer) 
			== ECacheFindResult::FIND_NOT_FOUND);
		
		item.index.itemKey++;
		BOOST_REQUIRE(cache.find(item.index.rangeID, item.index.itemKey, size, modTimeTag, buffer) 
			== ECacheFindResult::NOT_IN_CACHE);
		BOOST_REQUIRE(cache.remove(item.index));
		BOOST_REQUIRE(cache.find(item.index.rangeID, item.index.itemKey, size, modTimeTag, buffer) 
			== ECacheFindResult::FIND_NOT_FOUND);
		BOOST_REQUIRE(cache.replaceData(item, testData.c_str()) == false);
	}
	catch (...) {
		BOOST_CHECK_NO_THROW(throw);
	}
	
}

BOOST_AUTO_TEST_SUITE_END()
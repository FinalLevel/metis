///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Storage slice system unit tests
///////////////////////////////////////////////////////////////////////////////

#include <boost/test/unit_test.hpp>
#include "test_path.hpp"
#include "slice.hpp"
#include "range_index.hpp"
#include "dir.hpp"

using namespace fl::metis;
using fl::tests::TestPath;
using fl::fs::Directory;

BOOST_AUTO_TEST_SUITE( metis )

BOOST_AUTO_TEST_CASE (testSliceCreation)
{
	TestPath testPath("metis_slice");
	try
	{
		SliceManager sliceManager(testPath.path(), 0.05, 10);
		IndexEntry ie;
		ItemHeader &ih = ie.header;
		std::string testData("test");
		ih.level = 1;
		ih.rangeID = 1;
		ih.size = testData.size();
		BOOST_REQUIRE(sliceManager.add(testData.c_str(), ie));
		BOOST_REQUIRE(sliceManager.add(testData.c_str(), ie));
		BOOST_CHECK(testPath.countFiles("data") == 2);
		BOOST_CHECK(testPath.countFiles("index") == 2);
		ItemRequest ir;
		ir.pointer = ie.pointer;
		ir.size = ih.size;
		BString data;
		BOOST_REQUIRE(sliceManager.get(data, ir));
		BOOST_CHECK(data.size() == (BString::TSize)(ir.size + sizeof(ItemHeader)));
	}
	catch (...)
	{
		BOOST_CHECK_NO_THROW(throw);
	}
}


BOOST_AUTO_TEST_CASE (testSliceIndexLoad)
{
	TestPath testPath("metis_slice");
	BString levelPath;
	levelPath.sprintfSet("%s/1", testPath.path());
	Directory::makeDirRecursive(levelPath.c_str());
	const TItemModTime MOD_TIME = 2;
	const TRangeID RANGE_ID = 10;
	try
	{
		SliceManager sliceManager(levelPath.c_str(), 0.05, 10000);
		std::string testData("test");
		IndexEntry ie;
		ItemHeader &ih = ie.header;
		ih.status = 0;
		ih.rangeID = RANGE_ID;
		ih.level = 1;
		ih.subLevel = 1;
		ih.itemKey = 1;
		ih.timeTag.modTime = MOD_TIME;
		ih.timeTag.op = 2;
		ih.size = testData.size();
		
		BOOST_REQUIRE(sliceManager.add(testData.c_str(), ie));
		ih.itemKey = 2;
		BOOST_REQUIRE(sliceManager.add(testData.c_str(), ie));
		BOOST_REQUIRE(sliceManager.remove(ih, ie.pointer));
		
		ih.status = ST_ITEM_DELETED;
		ih.itemKey = 1;
		ih.timeTag.modTime = 1;
		BOOST_REQUIRE(sliceManager.add(testData.c_str(), ie));	

		ih.timeTag.modTime = MOD_TIME;
		ih.timeTag.op = 1;
		BOOST_REQUIRE(sliceManager.add(testData.c_str(), ie));	

	}
	catch (...)
	{
		BOOST_CHECK_NO_THROW(throw);
	}
	
	
	try
	{
		SliceManager sliceManager(levelPath.c_str(), 0.05, 10000);
		Index index;
		BOOST_REQUIRE(sliceManager.loadIndex(index));

		Range::Entry ie;
		BOOST_CHECK(index.find(RANGE_ID, 2, ie) == false);
		BOOST_CHECK(index.find(RANGE_ID, 1, ie) == true);
		BOOST_CHECK(ie.timeTag.modTime == MOD_TIME);
	}
	catch (...)
	{
		BOOST_CHECK_NO_THROW(throw);
	}
		
}


BOOST_AUTO_TEST_CASE (testSliceIndexRebuildFromData)
{
	TestPath testPath("metis_slice");
	BString levelPath;
	levelPath.sprintfSet("%s/1", testPath.path());
	Directory::makeDirRecursive(levelPath.c_str());
	const TItemModTime MOD_TIME = 2;
	const TRangeID RANGE_ID = 10;
	try
	{
		SliceManager sliceManager(levelPath.c_str(), 0.05, 10000);
		std::string testData("test");
		IndexEntry ie;
		ItemHeader &ih = ie.header;
		ih.status = 0;
		ih.rangeID = RANGE_ID;
		ih.level = 1;
		ih.subLevel = 1;
		ih.itemKey = 1;
		ih.timeTag.modTime = MOD_TIME;
		ih.timeTag.op = 2;
		ih.size = testData.size();
	
		BOOST_REQUIRE(sliceManager.add(testData.c_str(), ie));
		ih.itemKey = 2;
		BOOST_REQUIRE(sliceManager.add(testData.c_str(), ie));
		BOOST_REQUIRE(sliceManager.remove(ih, ie.pointer));
	
		BString indexFile;
		indexFile.sprintfSet("%s/index/%u", levelPath.c_str(), ie.pointer.sliceID);
		BOOST_REQUIRE(unlink(indexFile.c_str()) == 0);
	}
	catch (...)
	{
		BOOST_CHECK_NO_THROW(throw);
	}
	
	
	try
	{
		SliceManager sliceManager(levelPath.c_str(), 0.05, 10000);
		Index index;
		BOOST_REQUIRE(sliceManager.loadIndex(index));

		Range::Entry ie;
		BOOST_CHECK(index.find(RANGE_ID, 2, ie) == false);
		BOOST_CHECK(index.find(RANGE_ID, 1, ie) == true);
		BOOST_CHECK(ie.timeTag.modTime == MOD_TIME);
	}
	catch (...)
	{
		BOOST_CHECK_NO_THROW(throw);
	}
		
}

BOOST_AUTO_TEST_SUITE_END()

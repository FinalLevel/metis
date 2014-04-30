///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Storage slice management class
///////////////////////////////////////////////////////////////////////////////

#include "slice.hpp"
#include "metis_log.hpp"
#include "dir.hpp"
#include "config.hpp"
#include "range_index.hpp"



using namespace fl::metis;
using fl::fs::Directory;

void Slice::_openDataFile(BString &dataFileName)
{
	if (!_dataFd.open(dataFileName.c_str(), O_CREAT | O_RDWR)) {
		log::Fatal::L("Can't open slice dataFile %s\n", dataFileName.c_str());
		throw SliceError("Can't open slice dataFile");
	}
	TSize curSeek = _dataFd.seek(0, SEEK_END);
	if (curSeek < sizeof(SliceDataHeader)) 	{ // create new file
		_dataFd.seek(0, SEEK_SET);
		SliceDataHeader sh;
		sh.version = SliceDataHeader::CURRENT_VERSION;
		sh.sliceID = _sliceID;
		if (_dataFd.write(&sh, sizeof(sh)) != sizeof(sh)) {
			log::Fatal::L("Can't write slice dataFile header %s\n", dataFileName.c_str());
			throw SliceError("Can't write slice dataFile header");
		}
		_size = sizeof(sh);
	}	else {
		_dataFd.seek(0, SEEK_SET);
		SliceDataHeader sh;
		if (_dataFd.read(&sh, sizeof(sh)) != sizeof(sh)) {
			log::Fatal::L("Can't read slice dataFile header %s\n", dataFileName.c_str());
			throw SliceError("Can't read slice dataFile header");
		}
		if (sh.sliceID != _sliceID) {
			log::Fatal::L("SliceID mismatch in %s, %u != %u\n", dataFileName.c_str(), _sliceID, sh.sliceID);
			throw SliceError("SliceID mismatch");
		}
		_size = _dataFd.seek(0, SEEK_END);
	}
}

void Slice::_rebuildIndexFromData(BString &indexFileName)
{
	_indexFd.truncate(0);
	SliceIndexHeader sh;
	sh.version = SliceIndexHeader::CURRENT_VERSION;
	sh.sliceID = _sliceID;
	if (_indexFd.write(&sh, sizeof(sh)) != sizeof(sh)) {
		log::Fatal::L("Can't write slice indexFile header %s\n", indexFileName.c_str());
		throw SliceError("Can't write slice indexFile header");
	}
	if (_size <= sizeof(SliceDataHeader))
		return;
	
	log::Warning::L("Begin rebuild slice index file %s\n", indexFileName.c_str());

	Buffer buf(MAX_BUF_SIZE + sizeof(IndexEntry) * 2);
	_dataFd.seek(sizeof(SliceDataHeader), SEEK_SET);
	TSeek curSeek = sizeof(SliceDataHeader);
	while (curSeek < _size)	{
		IndexEntry &ie = *(IndexEntry*)buf.reserveBuffer(sizeof(IndexEntry));
		if (_dataFd.read(&ie.header, sizeof(ie.header)) != sizeof(ie.header)) {
			log::Fatal::L("Can't read an item header from slice dataFile while rebuilding %s\n", indexFileName.c_str());
			throw SliceError("Can't read an item header from slice dataFile");
		}

		auto resSeek = _dataFd.seek(ie.header.size, SEEK_CUR);
		if (resSeek <= 0) {
			log::Fatal::L("Can't seek slice dataFile while rebuilding %s\n", indexFileName.c_str());
			throw SliceError("Can't seek slice dataFile\n");
		}
		curSeek = resSeek;
		
		if (ie.header.status & ST_ITEM_DELETED) {
			buf.truncate(buf.writtenSize() - sizeof(IndexEntry));
		} else {
			ie.pointer.seek = curSeek;
			ie.pointer.sliceID = _sliceID;
		}
		if ((buf.writtenSize() >= MAX_BUF_SIZE) || (curSeek >= _size)) {
			if (_indexFd.write(buf.begin(), buf.writtenSize()) != (ssize_t)buf.writtenSize()) {
				log::Fatal::L("Can't write data to slice indexFile header %s\n", indexFileName.c_str());
				throw SliceError("Can't write data to slice indexFile header");
			}
			buf.clear();
		}
	}
}

void Slice::_openIndexFile(BString &indexFileName)
{
	if (!_indexFd.open(indexFileName.c_str(), O_CREAT | O_RDWR)) {
		log::Fatal::L("Can't open slice dataFile %s\n", indexFileName.c_str());
		throw SliceError("Can't open slice dataFile");
	}
	TSize curSeek = _indexFd.seek(0, SEEK_END);
	if (curSeek < sizeof(SliceIndexHeader)) {
		_rebuildIndexFromData(indexFileName);
	}	else {
		_indexFd.seek(0, SEEK_SET);
		try
		{
			SliceIndexHeader sh;
			if (_indexFd.read(&sh, sizeof(sh)) != sizeof(sh)) {
				log::Fatal::L("Can't read slice indexFile header %s\n", indexFileName.c_str());
				throw SliceError("Can't read slice indexFile header");
			}
			if (sh.sliceID != _sliceID) {
				log::Fatal::L("SliceID mismatch in %s, %u != %u\n", indexFileName.c_str(), _sliceID, sh.sliceID);
				throw SliceError("SliceID mismatch");
			}
			_indexFd.seek(0, SEEK_END);
		}
		catch (SliceError &er)
		{
			_rebuildIndexFromData(indexFileName);
		}
	}
}

Slice::Slice(const TSliceID sliceID, BString &dataFileName, BString &indexFileName)
	: _sliceID(sliceID)
{
	_openDataFile(dataFileName);
	_openIndexFile(indexFileName);
}

bool Slice::_writeItem(const char *data, IndexEntry &ie)
{
	if (_dataFd.seek(_size, SEEK_SET) != (off_t)_size) {
		log::Fatal::L("Can't seek slice dataFile %u\n", _sliceID);
		return false;
	}
	const ItemHeader &itemHeader = ie.header;
	if (_dataFd.write(&itemHeader, sizeof(itemHeader)) != sizeof(itemHeader)) {
		log::Fatal::L("Can't write header to slice dataFile %u\n", _sliceID);
		return false;
	}
	if (_dataFd.write(data, itemHeader.size) != (ssize_t)itemHeader.size) {
		log::Fatal::L("Can't write data to slice dataFile %u\n", _sliceID);
		return false;
	}

	ie.pointer.sliceID = _sliceID;
	ie.pointer.seek = _size;
		
	if (_indexFd.write(&ie, sizeof(ie)) != sizeof(ie))	{
		log::Fatal::L("Can't write index entry to slice indexFile %u\n", _sliceID);
		return false;
	}
	_size = _dataFd.seek(0, SEEK_CUR);
	return true;
}

bool Slice::_writeItem(File &putTmpFile, BString &buf, IndexEntry &ie)
{
	if (_dataFd.seek(_size, SEEK_SET) != (off_t)_size) {
		log::Fatal::L("Can't seek slice dataFile %u\n", _sliceID);
		return false;
	}
	const ItemHeader &itemHeader = ie.header;
	if (_dataFd.write(&itemHeader, sizeof(itemHeader)) != sizeof(itemHeader)) {
		log::Fatal::L("Can't write header to slice dataFile %u\n", _sliceID);
		return false;
	}
	auto leftSize = ie.header.size;
	while (leftSize > 0) {
		TItemSize chunkSize = MAX_BUF_SIZE;
		if (chunkSize > leftSize)
			chunkSize = leftSize;
		buf.clear();
		if (putTmpFile.read(buf.reserveBuffer(chunkSize), chunkSize) != (ssize_t)chunkSize) {
			log::Fatal::L("Can't read data from put tmp file %u\n", _sliceID);
			return false;
		}
		if (_dataFd.write(buf.c_str(), buf.size()) != (ssize_t)buf.size()) {
			log::Fatal::L("Can't write data to slice dataFile %u\n", _sliceID);
			return false;
		}
		leftSize -= chunkSize;
	}
	ie.pointer.sliceID = _sliceID;
	ie.pointer.seek = _size;
		
	if (_indexFd.write(&ie, sizeof(ie)) != sizeof(ie))	{
		log::Fatal::L("Can't write index entry to slice indexFile %u\n", _sliceID);
		return false;
	}
	_size = _dataFd.seek(0, SEEK_CUR);
	return true;
}

bool Slice::add(File &putTmpFile, BString &buf, IndexEntry &ie)
{
	AutoReadWriteLockWrite autoSyncWrite(&_sync);
	TSeek curIndexSeek = _indexFd.seek(0, SEEK_CUR);
	if (!_writeItem(putTmpFile, buf, ie)) {
		_dataFd.truncate(_size);
		_indexFd.truncate(curIndexSeek);
		return false;
	}
	return true;
}

bool Slice::add(const char *data, IndexEntry &ie)
{
	AutoReadWriteLockWrite autoSyncWrite(&_sync);
	TSeek curIndexSeek = _indexFd.seek(0, SEEK_CUR);
	if (!_writeItem(data, ie)) {
		_dataFd.truncate(_size);
		_indexFd.truncate(curIndexSeek);
		return false;
	}
	return true;
}

bool Slice::remove(const ItemHeader &ih, const ItemPointer &pointer)
{
	AutoReadWriteLockWrite autoSyncRead(&_sync);
	if (pointer.seek >= _size) {
		log::Fatal::L("Can't remove out of range seek sliceID %u, seek %u\n", _sliceID, pointer.seek);
		return false;
	}
	ItemHeader diskItemHeader;
	if (_dataFd.pread(&diskItemHeader, sizeof(diskItemHeader), pointer.seek) != sizeof(diskItemHeader)) {
		log::Fatal::L("Can't read deleting the item header file from seek sliceID %u, seek %u\n", _sliceID, pointer.seek);
		return false;
	}
	if ((diskItemHeader.rangeID != ih.rangeID) || (diskItemHeader.itemKey != ih.itemKey)) {
		log::Fatal::L("Can't delete the item: Headers are different sliceID %u, seek %u\n", _sliceID, pointer.seek);
		return false;		
	}
	if (diskItemHeader.status & ST_ITEM_DELETED) {
		log::Warning::L("The item is already deleted sliceID %u, seek %u\n", _sliceID, pointer.seek);
		return true;		
	}
	if (diskItemHeader.timeTag <= ih.timeTag) {
		diskItemHeader.status |= ST_ITEM_DELETED;
		if (_dataFd.pwrite(&diskItemHeader.status, sizeof(diskItemHeader.status), 
			pointer.seek) != sizeof(diskItemHeader.status)) {
			log::Fatal::L("Can't write delete status to the file from seek sliceID %u, seek %u\n", _sliceID, pointer.seek);
			return false;
		}
		IndexEntry ie;
		diskItemHeader.timeTag = ih.timeTag;
		ie.header = diskItemHeader;
		ie.pointer = pointer;

		if (_indexFd.write(&ie, sizeof(ie)) != sizeof(ie))	{
			log::Fatal::L("Can't write remove index entry to slice indexFile %u\n", _sliceID);
			return false;
		}
	
		return true;
	} else {
		log::Fatal::L("Can't delete the newer item\n", _sliceID, pointer.seek);
		return false;				
	}
}

bool Slice::get(BString &data, const TItemSize dataSeek, const TItemSize requestSeek, const TItemSize requestSize)
{
	AutoReadWriteLockRead autoSyncRead(&_sync);
	TSeek seek = dataSeek + requestSeek + sizeof(ItemHeader);
	if ((seek +  requestSize) >= _size) {
		log::Fatal::L("Can't get out of range sliceID %u, seek %u\n", _sliceID, (dataSeek +  requestSeek +  requestSize));
		return false;
	}
	if (_dataFd.pread(data.reserveBuffer(requestSize), requestSize, seek) != requestSize) {
		log::Fatal::L("Can't read data file from seek sliceID %u, seek %u\n", _sliceID, seek);
		return false;
	}
	return true;

}

bool Slice::get(BString &data, const ItemRequest &item)
{
	AutoReadWriteLockRead autoSyncRead(&_sync);
	if (item.pointer.seek >= _size) {
		log::Fatal::L("Can't get out of range sliceID %u, seek %u\n", _sliceID, item.pointer.seek);
		return false;
	}
	ssize_t readSize = item.size + sizeof(ItemHeader);
	if (_dataFd.pread(data.reserveBuffer(readSize), readSize, item.pointer.seek) != readSize) {
		log::Fatal::L("Can't read data file from seek sliceID %u, seek %u\n", _sliceID, item.pointer.seek);
		return false;
	}
	return true;
}

bool Slice::loadIndex(Index &index, Buffer &buf)
{
	auto leftSize = _indexFd.fileSize();
	_indexFd.seek(sizeof(SliceIndexHeader), SEEK_SET);
	leftSize -= sizeof(SliceIndexHeader);
	while (leftSize > 0) {
		static uint32_t CHUNK_SIZE = sizeof(IndexEntry) * 100000;
		
		ssize_t readSize = CHUNK_SIZE;
		if (readSize > leftSize)
			readSize = leftSize;
		buf.clear();
		if (_indexFd.read(buf.reserveBuffer(readSize), readSize) != readSize) {
			log::Fatal::L("Can't read data index from sliceID %u\n", _sliceID);
			return false;
		}
		leftSize -= readSize;
		while (buf.readPos() < buf.writtenSize()) {
			IndexEntry &ie = *(IndexEntry*)buf.mapBuffer(sizeof(IndexEntry));
			if (ie.header.status & ST_ITEM_DELETED)
				index.remove(ie.header);
			else
				index.addNoLock(ie);
		}
	}
	return true;
}

SliceManager::SliceManager(const char *path, const double minFree, const TSize maxSliceSize)
	: _path(path), _minFree(minFree), _leftSpace(0), _maxSliceSize(maxSliceSize)
{
	if (!_recalcSpace())
		throw SliceError("Can't recalculate disk space");
	_init();
}


void SliceManager::_formDataPath(BString &path)
{
	path.sprintfSet("%s/data", _path.c_str());
}

void SliceManager::_formIndexPath(BString &path)
{
	path.sprintfSet("%s/index", _path.c_str());
}

void SliceManager::_init()
{
	BString dataPath;
	_formDataPath(dataPath);
	Directory::makeDirRecursive(dataPath.c_str());
	
	BString indexPath;
	_formIndexPath(indexPath);
	Directory::makeDirRecursive(indexPath.c_str());
	
	try
	{
		Directory dir(dataPath.c_str());
		BString dataFileName;
		BString indexFileName;
		while (dir.next()) {
			if (dir.name()[0] == '.')
				continue;
			TSliceID sliceID = strtoul(dir.name(), NULL, 10);
			dataFileName.sprintfSet("%s/%u", dataPath.c_str(), sliceID);
			indexFileName.sprintfSet("%s/%u", indexPath.c_str(), sliceID);
			TSlicePtr slice(new Slice(sliceID, dataFileName, indexFileName));
			if (sliceID >= _slices.size())
				_slices.resize(sliceID + 1);
			_slices[sliceID] = slice;
		}
		return;
	}
	catch (Directory::Error &er)
	{
		log::Error::L("Can't open slice storage data path %s\n", dataPath.c_str());
	}
	throw SliceError("Can't initialize sliceManager");
}

bool SliceManager::findWriteSlice(const TItemSize size)
{
	if ((int64_t)size > _leftSpace)
		return false;
	
	AutoMutex autoSync(&_sync);

	if ((_writeSlice.get() == NULL) || ((_writeSlice->size() + size) > _maxSliceSize))	{
		if (!_addWriteSlice(size))
			return false;
	}
	return true;
}

bool SliceManager::add(File &putTmpFile, BString &buf, IndexEntry &ie)
{
	if (!findWriteSlice(ie.header.size))
		return false;
	if (_writeSlice->add(putTmpFile, buf, ie))
	{
		_leftSpace = __sync_sub_and_fetch(&_leftSpace, ie.header.size);
		return true;
	} else {
		return false;
	}		
}

bool SliceManager::add(const char *data, IndexEntry &ie)
{
	if (!findWriteSlice(ie.header.size))
		return false;

	if (_writeSlice->add(data, ie))
	{
		_leftSpace = __sync_sub_and_fetch(&_leftSpace, ie.header.size);
		return true;
	} else {
		return false;
	}
}

bool SliceManager::remove(const ItemHeader &ih, const ItemPointer &pointer)
{
	AutoMutex autoSync(&_sync);
	if (pointer.sliceID >= _slices.size())
	{
		log::Error::L("Can't get slice %u\n", pointer.sliceID);
		return false;
	}
	TSlicePtr slice = _slices[pointer.sliceID];
	autoSync.unLock();
	return slice->remove(ih, pointer);
	
}

bool SliceManager::get(BString &data, const ItemPointer &pointer, const TItemSize seek, const TItemSize size)
{
	AutoMutex autoSync(&_sync);
	if (pointer.sliceID >= _slices.size())
	{
		log::Error::L("Can't get slice %u\n", pointer.sliceID);
		return false;
	}
	TSlicePtr slice = _slices[pointer.sliceID];
	autoSync.unLock();
	return slice->get(data, pointer.seek, seek, size);

}

bool SliceManager::get(BString &data, const ItemRequest &item)
{
	AutoMutex autoSync(&_sync);
	if (item.pointer.sliceID >= _slices.size())
	{
		log::Error::L("Can't get slice %u\n", item.pointer.sliceID);
		return false;
	}
	TSlicePtr slice = _slices[item.pointer.sliceID];
	autoSync.unLock();
	return slice->get(data, item);
}

bool SliceManager::loadIndex(class Index &index)
{
	Buffer buf;
	for (auto slice = _slices.begin(); slice != _slices.end(); slice++)
	{
		if (!(*slice)->loadIndex(index, buf))
			return false;
	}
	return true;
}

bool SliceManager::_addWriteSlice(const TSize size)
{
	TSliceID sliceID = 0;
	for (auto slice = _slices.begin(); slice != _slices.end(); slice++, sliceID++) {
		if (slice->get() == NULL)
			break;
		if ((slice->get()->size() + size) < _maxSliceSize) {
			_writeSlice = *slice;
			return true;
		}
	}
	BString dataFileName;
	_formDataPath(dataFileName);
	dataFileName.sprintfAdd("/%u", sliceID);

	BString indexFileName;
	_formIndexPath(indexFileName);
	indexFileName.sprintfAdd("/%u", sliceID);

	TSlicePtr slicePtr(new Slice(sliceID, dataFileName, indexFileName));
	if (sliceID >= _slices.size())
		_slices.resize(sliceID + 1);
	_slices[sliceID] = slicePtr;
	_writeSlice = slicePtr;
	return true;
}

bool SliceManager::_recalcSpace()
{
	uint64_t totalSpace;
	uint64_t freeSpace;
	if (!Directory::getDiskSize(_path.c_str(), totalSpace, freeSpace))
		return false;
	
	uint64_t reserveSpace = (totalSpace * _minFree);
	if (freeSpace < reserveSpace)
		_leftSpace = 0;
	else
		_leftSpace = freeSpace - reserveSpace;
	return true;
}

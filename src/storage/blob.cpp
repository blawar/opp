#include <sx/libsx.h>
#include "cerberus.h"

#define BLOCK_SIZE 512000

void error(string e);

TBLOB::TBLOB()
{
}

TBLOB::~TBLOB()
{
}

int64 TBLOB::insert(int8* pbBuffer, TBLOB_ROW_TYPE ulSize)
{
	if(!ulSize) return 0;
	int64 cur_pos = size();
	if(!cur_pos) cur_pos = 1;
	//error(string("inserting ") + boost::lexical_cast<string>(cur_pos));
	realloc(cur_pos + ulSize + sizeof(TBLOB_ROW_TYPE));
	if (cur_pos == size())
		throw "realloc failed";

	writebuf(cur_pos, &ulSize, sizeof(ulSize));
	if (pbBuffer)
	{
		writebuf(cur_pos + sizeof(TBLOB_ROW_TYPE), pbBuffer, ulSize);
	}
	return cur_pos;
}

int64 TBLOB::update(int64 ulAddr, int8* pbBuffer, TBLOB_ROW_TYPE ulSize)
{
	ulAddr = resize(ulAddr, ulSize);
	writebuf(ulAddr + sizeof(TBLOB_ROW_TYPE), pbBuffer, ulSize);
	return ulAddr;
}

int64 TBLOB::write(int64 ulAddr, int8* pbBuffer, TBLOB_ROW_TYPE ulSize, int64 offset)
{
	writebuf(ulAddr + sizeof(TBLOB_ROW_TYPE) + offset, pbBuffer, ulSize);
	return ulAddr;
}

int8* TBLOB::read(int64 ulAddr, string &buffer, int64 offset, int64 length)
{
	TBLOB_ROW_TYPE ulSize;
	readbuf(ulAddr, &ulSize, sizeof(ulSize));
	if(!length) length = ulSize - offset;
	buffer.resize(length);
	readbuf(ulAddr + sizeof(TBLOB_ROW_TYPE)+offset, (void*)buffer.c_str(), length);
	return (int8*)buffer.c_str();
}

bool TBLOB::remove(int64 ulAddr)
{
	//error(string("Removing blob: ") + boost::lexical_cast<string>(ulAddr));
	TBLOB_ROW_TYPE uiCurrentSz=0;
	//writebuf(ulAddr, &uiCurrentSz, sizeof(uiCurrentSz));
	return true;
	readbuf(ulAddr, &uiCurrentSz, sizeof(uiCurrentSz));
	for (int64 i = 0; i < uiCurrentSz/* + sizeof(TBLOB_ROW_TYPE)*/; i++)
		writebuf(ulAddr + i, (void*)"\xFF", 1);
	return true;
}

int64 TBLOB::resize(int64 dwAddr, TBLOB_ROW_TYPE dwSize)
{
	TBLOB_ROW_TYPE dwOldSize=0;
	readbuf(dwAddr, &dwOldSize, sizeof(dwOldSize));

	if (dwSize > dwOldSize)
	{
		int64 dwNewAddr = insert(0, dwSize);
		void* p = malloc(dwSize + sizeof(dwOldSize));
		readbuf(dwAddr + sizeof(dwOldSize), p, dwOldSize);
		memset((char*)p + dwOldSize, 0, dwSize - dwOldSize);
		*(TBLOB_ROW_TYPE*)p = dwSize;
		writebuf(dwNewAddr, p, dwSize + sizeof(dwOldSize));
		if(dwNewAddr != dwAddr) remove(dwAddr);
		free(p);
		return dwNewAddr;
	}
	else //quick resize down
	{
		//void* p = malloc(dwOldSize - dwSize);
		//memset(p, 0, dwOldSize - dwSize);
		//writebuf(dwAddr + dwSize + sizeof(dwSize), p, dwOldSize - dwSize);
		//free(p);

		//writebuf(dwAddr, &dwSize, sizeof(dwSize));
		return dwAddr;
		/*
		int64 dwNewAddr = insert(0, dwSize);
		void* p = malloc(dwSize + sizeof(dwOldSize));
		readbuf(dwAddr + sizeof(dwOldSize), p, dwSize);
		*(TBLOB_ROW_TYPE*)p = dwSize;
		writebuf(dwNewAddr, p, dwSize + sizeof(dwOldSize));
		//remove(dwAddr);
		free(p);
		return dwAddr;
		return insert(0, dwSize);*/
	}
}

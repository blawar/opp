#ifndef TBLOB_H
#define TBLOB_H

#include "../cerberus.h"
#include <vector>
#include <iostream>
#include <string>
#include <string.h>
#include <boost/thread.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/regex.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <sx/mmfile.h>

#define BLOCK_SIZE 512000
#define TBLOB_ROW_TYPE	int64

class TBLOB : public MMFILE
{
public:
	TBLOB();
	~TBLOB();
	bool close();
	int64 insert(int8* pbBuffer, TBLOB_ROW_TYPE ulSize);
	int64 update(int64 ulAddr, int8* pbBuffer, TBLOB_ROW_TYPE ulSize);
	int64 write(int64 ulAddr, int8* pbBuffer, TBLOB_ROW_TYPE ulSize, int64 offset);
	int8* read(int64 ulAddr, string &buffer, int64 offset=0, int64 length=0);
	int64 resize(int64 dwAddr, TBLOB_ROW_TYPE dwSize);
	bool remove(int64 ulAddr);
};

#endif

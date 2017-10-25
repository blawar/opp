#include "field.h"

FIELD::FIELD()
{
	blob = NULL;
	pFile = NULL;
}

FIELD::~FIELD()
{
}

void FIELD::load(string file)
{
	string szData;
	if(file.size()) szData = file + ".opd";
	else szData = "";

	pFile = new MMFILE();
	pFile->load(szData.c_str());

	if (field_type == t_text || field_type == t_binary || field_type == t_varchar || field_type == t_varbyte || field_type == t_stream)
	{
		if(file.size()) szData = file + ".blob";
		else szData = "";
		blob = new TBLOB;
		blob->load(szData.c_str());
	}
}

void FIELD::write_byte(int64 primary_key, int8 value)
{
    primary_key--;
    int64 offset = primary_key * sizeof(int8);
    pFile->realloc(offset+sizeof(int8));
	pFile->writebuf(offset, &value, sizeof(value));
}

void FIELD::write_byte(int64 primary_key, int8* value)
{
        primary_key--;
        int64 offset = primary_key * field_size;
        pFile->realloc(offset+field_size);
		pFile->writebuf(offset, value, field_size);
}

void FIELD::write_short(int64 primary_key, int16 value)
{
        primary_key--;
        int64 offset = primary_key * sizeof(int16_t);
        pFile->realloc(offset+sizeof(int16_t));
		pFile->writebuf(offset, &value, sizeof(value));
}

void FIELD::write_int(int64 primary_key, int32 value)
{
    primary_key--;
    int64 offset = primary_key * sizeof(int);
    pFile->realloc(offset+sizeof(int));
    pFile->writebuf(offset, &value, sizeof(value));
}

void FIELD::write_bit(int64 primary_key, bool value)
{
	int64 offset = primary_key / 8 + sizeof(int64);
	int8 b;

    pFile->readbuf(offset, &b, sizeof(b));

	if(value) switch(primary_key % 8)
	{
		case 0: b |= 1; break;
		case 1: b |= 2; break;
		case 2: b |= 4; break;
		case 3: b |= 8; break;
		case 4: b |= 16; break;
		case 5: b |= 32; break;
		case 6: b |= 64; break;
		case 7: b |= 128; break;
	}
	else switch(primary_key % 8)
	{
		case 0: b &= ~(int8)1; break;
		case 1: b &= ~(int8)2; break;
		case 2: b &= ~(int8)4; break;
		case 3: b &= ~(int8)8; break;
		case 4: b &= ~(int8)16; break;
		case 5: b &= ~(int8)32; break;
		case 6: b &= ~(int8)64; break;
		case 7: b &= ~(int8)128; break;
	}

	pFile->writebuf(offset, &b, sizeof(b));
}

void FIELD::write_long(int64 primary_key, int64 value)
{
    primary_key--;
    int64 offset = primary_key * sizeof(value);
    pFile->realloc(offset+sizeof(value));
    pFile->writebuf(offset, &value, sizeof(value));
}

int8 FIELD::read_byte(int64 primary_key)
{
	int8 a;
    primary_key--;
    int64 offset = size() * primary_key;
    pFile->readbuf(offset, &a, sizeof(a));
    return a;
}

int8* FIELD::read_bytes(int64 primary_key, string *buffer)
{
	int64 ulSize = field_size;

    primary_key--;
    int64 offset = ulSize * primary_key;

	buffer->resize(ulSize);
	return (int8*)(pFile->readbuf(offset, (void*)(buffer->c_str()), ulSize));
}

int16_t FIELD::read_short(int64 primary_key)
{
	int16 r=0;
    primary_key--;
    int64 offset = size() * primary_key;
    pFile->readbuf(offset, &r, sizeof(r));
    return r;
}

int32 FIELD::read_int(int64 primary_key)
{
	int32 r=0;
	primary_key--;
	int64 offset = size() * primary_key;
	pFile->readbuf(offset, &r, sizeof(r));
	return r;
}

bool FIELD::read_bit(int64 primary_key)
{
	int64 offset = (primary_key / 8) + sizeof(int64);
	int8 b;
	pFile->readbuf(offset, &b, sizeof(b));
	switch(primary_key % 8)
	{
		case 0: return (b & 1) != 0;
		case 1: return (b & 2) != 0;
		case 2: return (b & 4) != 0;
		case 3: return (b & 8) != 0;
		case 4: return (b & 16) != 0;
		case 5: return (b & 32) != 0;
		case 6: return (b & 64) != 0;
		case 7: return (b & 128) != 0;
	}
	return false;
}

int64 FIELD::read_long(int64 primary_key)
{
	int64 r=0;
    primary_key--;
    int64 offset = sizeof(r) * primary_key;
    pFile->readbuf(offset, &r, sizeof(r));
    return r;
}

void FIELD::write_blob(int64 primary_key, int8* val, TBLOB_ROW_TYPE ulSize)
{
	int64 dwAddr = read_long(primary_key);

	if(!ulSize)
	{
		write_long(primary_key, 0);
		return;
	}

	if(dwAddr)
	{
		dwAddr = blob->update(dwAddr, val, ulSize);
		write_long(primary_key, dwAddr);
	}
	else
	{
		dwAddr = blob->insert(val, ulSize);
		write_long(primary_key, dwAddr);
	}
}

int8* FIELD::read_blob(int64 primary_key, string &buffer)
{
	int64 offset = read_long(primary_key);

	if(!offset) return (int8*)"";
	if(!blob) return (int8*)"";

	return blob->read(offset, buffer);
}

void FIELD::create_stream(int64 primary_key, TBLOB_ROW_TYPE ulSize)
{
	int64 dwAddr = read_long(primary_key);

	if(!ulSize)
	{
		write_long(primary_key, 0);
		return;
	}

	if(dwAddr) // The stream has already been created?
	{
		return;
	}

	int32 dwHeaderSize = ulSize / 131072 / 8 + 2;
	dwAddr = blob->insert(NULL, ulSize + dwHeaderSize + sizeof(dwHeaderSize));
	blob->write(dwAddr, (int8*)&dwHeaderSize, sizeof(dwHeaderSize), 0);
	write_long(primary_key, dwAddr);
}

void FIELD::setbit(int8* p, int64 offset, bool value)
{
	int64 base = offset / 8;
	//error(S("writing bit: ") + S(offset) + " @base " + S(base) + ":" + S(offset % 8));
	if(value) switch(offset % 8)
	{
		case 0: p[base] |= 1; break;
		case 1: p[base] |= 2; break;
		case 2: p[base] |= 4; break;
		case 3: p[base] |= 8; break;
		case 4: p[base] |= 16; break;
		case 5: p[base] |= 32; break;
		case 6: p[base] |= 64; break;
		case 7: p[base] |= 128; break;
	}
	else switch(offset % 8)
	{
		case 0: p[base] &= ~(int8)1; break;
		case 1: p[base] &= ~(int8)2; break;
		case 2: p[base] &= ~(int8)4; break;
		case 3: p[base] &= ~(int8)8; break;
		case 4: p[base] &= ~(int8)16; break;
		case 5: p[base] &= ~(int8)32; break;
		case 6: p[base] &= ~(int8)64; break;
		case 7: p[base] &= ~(int8)128; break;
	}
}

bool FIELD::getbit(int8* p, int64 offset)
{
	int64 base = offset / 8;
	switch(offset % 8)
	{
		case 0: return (p[base] & 1) != 0;
		case 1: return (p[base] & 2) != 0;
		case 2: return (p[base] & 4) != 0;
		case 3: return (p[base] & 8) != 0;
		case 4: return (p[base] & 16) != 0;
		case 5: return (p[base] & 32) != 0;
		case 6: return (p[base] & 64) != 0;
		case 7: return (p[base] & 128) != 0;
	}
	return false;
}

void FIELD::write_stream(int64 primary_key, int8* val, TBLOB_ROW_TYPE ulSize, int64 offset)
{
	int64 start=0, stop=0, aligned_offset=0, aligned_size=0; 
	int64 dwAddr = read_long(primary_key);

	if(!ulSize)
	{
		write_long(primary_key, 0);
		return;
	}

	if(dwAddr)
	{
		int32 dwHeaderSize;
		string s;
		blob->read(dwAddr, s, 0, sizeof(int32));
		dwHeaderSize = *(int32*)s.c_str();

		aligned_offset  = (offset / 131072) * 131072;
		//if(aligned_offset < offset) aligned_offset += 131072;
		if(aligned_offset <  offset + ulSize)
		{
			s.resize(dwHeaderSize);
			blob->readbuf(dwAddr+sizeof(TBLOB_ROW_TYPE)+sizeof(dwHeaderSize)+1, (void*)s.c_str(), dwHeaderSize);

			start = aligned_offset / 131072;

			if(ulSize < 131072)
			{
				setbit((int8*)s.c_str(), start, true);
			}
			else
			{
				aligned_size  = int64(ceil(double(ulSize / ulSize))) * 131072;

				stop = start + (aligned_size / 131072);
				while(start < stop)
				{
					setbit((int8*)s.c_str(), start, true);
					start++;
				}
			}
			blob->writebuf(dwAddr+sizeof(TBLOB_ROW_TYPE)+sizeof(dwHeaderSize)+1, (void*)s.c_str(), s.size());
		}
		blob->write(dwAddr+dwHeaderSize+sizeof(dwHeaderSize)+offset, val, ulSize, 0);
		write_long(primary_key, dwAddr);
	}
	else // need to initialize the stream first!
	{
		return;
	}
}

bool FIELD::stream_is_ready(int64 primary_key, int64 offset, int64 length)
{
/*	string s;
	int64 aligned_offset  = (offset / 131072) * 131072, aligned_size  = ((length / 131072)+1) * 131072;
	int64 start = aligned_offset / 131072;
	int64 stop = start + (aligned_size / 131072);

	while(start < stop)
	{
		if(!readbit
		start++;
	}*/
	return false;
}

int8* FIELD::read_stream(int64 primary_key, string &buffer, int64 offset, int64 length, int timeout)
{
	buffer.erase();
	int64 dwAddr = read_long(primary_key);

	if(!dwAddr) return (int8*)"";
	if(!blob) return (int8*)"";

	int32 dwHeaderSize;
	blob->read(dwAddr, buffer, 0, sizeof(int32));
	dwHeaderSize = *(int32*)buffer.c_str();

	TBLOB_ROW_TYPE sz=0;
	blob->readbuf(dwAddr, (void*)&sz, sizeof(sz));

	if(!length)
	{
		length = sz-sizeof(sz)-dwHeaderSize-offset;
	}
	else if(offset + length + sizeof(sz) + dwHeaderSize > sz)
	{
		if(offset + sizeof(sz) + dwHeaderSize > sz)
		{
			buffer.erase();
			return (int8*)buffer.c_str();
		}
		length = sz - offset - sizeof(sz) - dwHeaderSize;
	}

	int64 aligned_offset  = (offset / 131072) * 131072, aligned_size  = ((length / 131072)+1) * 131072;
	int64 start = aligned_offset / 131072;
	int64 stop = start + (aligned_size / 131072);

	buffer.resize(dwHeaderSize);

	long error_count = 0;
	while(1)
	{
		start = aligned_offset / 131072;

		blob->readbuf(dwAddr+sizeof(TBLOB_ROW_TYPE)+sizeof(dwHeaderSize)+1, (void*)buffer.c_str(), dwHeaderSize);
		while(start < stop)
		{
			if(!getbit((int8*)buffer.c_str(), start))
			{
				//error(S("blocking while trying to read ") + S(length) + " bytes @ " + S(offset));
				break;
			}
			start++;
		}
		if(start == stop) break;

		if(error_count++ > timeout / 50)
		{
			buffer.erase();
			return (int8*)buffer.c_str();
		}

		boost::this_thread::sleep(boost::posix_time::milliseconds(50));
	}

	buffer.resize(length);
	blob->readbuf(dwAddr+sizeof(TBLOB_ROW_TYPE)+offset+dwHeaderSize+sizeof(int32), (void*)buffer.c_str(), length);

	return (int8*)buffer.c_str();
}

TBLOB_ROW_TYPE FIELD::get_stream_size(int64 primary_key)
{
	int64 dwAddr = read_long(primary_key);

	if(!dwAddr) return 0;
	if(!blob) RESET_STREAM(primary_key);

	TBLOB_ROW_TYPE ulSize=0;
	int32 dwHeaderSize=0;
	blob->readbuf(dwAddr, &ulSize, sizeof(ulSize));
	if(!ulSize) RESET_STREAM(primary_key);
	blob->readbuf(dwAddr+sizeof(TBLOB_ROW_TYPE), &dwHeaderSize, sizeof(dwHeaderSize));
	if(!dwHeaderSize) RESET_STREAM(primary_key);

	return ulSize - sizeof(TBLOB_ROW_TYPE) - dwHeaderSize;
}

float FIELD::get_stream_percent(int64 primary_key)
{
	string buf;
	int64 dwAddr = read_long(primary_key);


	if(!dwAddr) return 0;
	if(!blob) RESET_STREAM(primary_key);

	TBLOB_ROW_TYPE ulSize=0;
	int32 dwHeaderSize=0;
	blob->readbuf(dwAddr+sizeof(TBLOB_ROW_TYPE), &dwHeaderSize, sizeof(dwHeaderSize));
	if(!dwHeaderSize) RESET_STREAM(primary_key);
	buf.resize(dwHeaderSize-1);

	int8* p = (int8*)buf.c_str();
	blob->readbuf(dwAddr, &ulSize, sizeof(ulSize));
	if(!ulSize) RESET_STREAM(primary_key);
	blob->readbuf(dwAddr+sizeof(TBLOB_ROW_TYPE)+sizeof(dwHeaderSize)+1, p, dwHeaderSize-1);

	int64 max = (ulSize - sizeof(TBLOB_ROW_TYPE) - dwHeaderSize) / 131072, found=0;

	if(!max) return 0.0f;

	for(int64 i=0; i < max; i++)
	{
		if(getbit(p, i)) found++;
	}

	return float(found * 100 / max);
}

void FIELD::get_stream_header(int64 primary_key, string &header)
{
	header.resize(0);
	int64 dwAddr = read_long(primary_key);

	if(!dwAddr) return;
	if(!blob) return;

	TBLOB_ROW_TYPE ulSize=0;
	int32 dwHeaderSize=0;

	blob->readbuf(dwAddr+sizeof(TBLOB_ROW_TYPE), &dwHeaderSize, sizeof(dwHeaderSize));
	if(dwHeaderSize > 0)
	{
		header.resize(dwHeaderSize-1);

		blob->readbuf(dwAddr, &ulSize, sizeof(ulSize));
		blob->readbuf(dwAddr+sizeof(TBLOB_ROW_TYPE)+sizeof(dwHeaderSize)+1, (char*)header.c_str(), dwHeaderSize-1);
	}
}

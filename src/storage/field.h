#ifndef SB_FIELD_H
#define SB_FIELD_H

#include "../cerberus.h"
#include <string>
#include "blob.h"
#include <sx/libsx.h>

#define STORAGE_SANITY_CHECK(column) if(column >= pfields.size() || !pfields[column].pFile) throw 0;
#define RESET_STREAM(pk) { write_long(pk, 0); return 0; }

class FIELD
{
public:
	FIELD();
	~FIELD();
	void load(string file);
	string name;
	int64 auto_increment;
	bool field_unsigned;
	NULLTYPE null_type;
	FIELDTYPE field_type;
	int64 field_size;
	MMFILE* pFile;
	TBLOB* blob;

	int64 size() { return field_size; }
	FIELDTYPE type() { return field_type; }

	void write_byte(int64 primary_key, int8 value);
	void write_byte(int64 primary_key, int8* value);
	void write_short(int64 primary_key, int16 value);
	void write_bit(int64 primary_key, bool value);
	void write_int(int64 primary_key, int32 value);
	void write_long(int64 primary_key, int64 value);
	void write_blob(int64 primary_key, int8* val, TBLOB_ROW_TYPE ulSize);

	int8 read_byte(int64 primary_key);
	int8* read_bytes(int64 primary_key, string *buffer);
	int16_t read_short(int64 primary_key);
	bool read_bit(int64 primary_key);
	int32 read_int(int64 primary_key);
	int64 read_long(int64 primary_key);
	int8* read_blob(int64 primary_key, string &buffer);

	void setbit(int8* p, int64 offset, bool value);
	bool getbit(int8* p, int64 offset);

	void create_stream(int64 primary_key, TBLOB_ROW_TYPE ulSize);
	void write_stream(int64 primary_key, int8* val, TBLOB_ROW_TYPE ulSize, int64 offset=0);
	int8* read_stream(int64 primary_key, string &buffer, int64 offset=0, int64 length=0, int timeout=1000);
	TBLOB_ROW_TYPE get_stream_size(int64 primary_key);
	float get_stream_percent(int64 primary_key);
	void get_stream_header(int64 primary_key, string &header);
	bool stream_is_ready(int64 primary_key, int64 offset, int64 length);
private:
};

#endif

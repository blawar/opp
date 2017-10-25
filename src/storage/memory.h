#ifndef MEMORY_H
#define MEMORY_H

#include "opp/cerberus.h"
#include "storage.h"
#include <sx/mmfile.h>
class STORAGE;
class MEMORY : public STORAGE, public MMFILE
{
public:
	MEMORY(Cerberus* cp);
	~MEMORY();
	bool load(const char* pszTable);
	FIELD* add_field(string name, FIELDTYPE field_type, int64 size)
	{
		return STORAGE::add_field(name, field_type, size, false);
	}
	bool resize(int64 ulColumnId, int64 ulSize);
	bool create(string table, std::vector<FIELD> &fields);
};

#endif

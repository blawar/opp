#include "sx/libsx.h"
#include "opp/cerberus.h"

MEMORY::MEMORY(Cerberus* cp)
{
	pcParent = cp;
	num_rows = ulRowSize = 0;

	add_field("$id", t_int, 4);
}

MEMORY::~MEMORY()
{
    free();
}

bool MEMORY::load(const char* pszTable)
{
	return true;
}

bool MEMORY::resize(int64 ulColumnId, int64 ulSize)
{
        if(pfields[ulColumnId].pFile) return pfields[ulColumnId].pFile->realloc(ulSize);
		else return false;
}

bool MEMORY::create(string table, std::vector<FIELD> &fields)
{
	return true;
}

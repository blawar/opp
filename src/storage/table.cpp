#include "opp/cerberus.h"
#include "storage.h"

TABLE::TABLE(Cerberus* cp, const char* pszDatabase, const char* pszTable)
{
	pcParent = cp;
	num_rows = ulRowSize = 0;
	load(pszDatabase, pszTable);
}

TABLE::~TABLE()
{
	free();
}

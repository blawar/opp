#ifndef TABLE_H
#define TABLE_H

class TABLE : public STORAGE
{
public:
	TABLE(Cerberus* p, const char* pszDatabase, const char* pszTable);
	~TABLE();
};

#endif

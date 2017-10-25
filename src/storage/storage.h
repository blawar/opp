#ifndef STORAGE_H
#define STORAGE_H

#define BLOCK_SIZE 512000

class FIELD;
class Cerberus;
class STORAGE;
class TABLE;
class MEMORY;

#include <vector>
#include <string>
#include <boost/thread.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/regex.hpp>
#include <boost/lexical_cast.hpp>
#include "opp/storage/blob.h"
#include "strings.h"
#include "opp/storage/field.h"

namespace fs = boost::filesystem;

class FIELDS : public std::vector<FIELD*>
{
public:
	FIELD& operator[](unsigned long i)
	{
		return *at(i);
	}

	void load(string file);
	void save(string file);
};

class STORAGE
{
public:
	Cerberus* pcParent;
	string szName;
	string szCurrentDb;
	FIELDS pfields;
	int64 ulRowSize;
	int64 num_rows;

	unsigned char fieldSize(FIELD &f);
	unsigned char fieldSize(FIELD *f);
public:
	STORAGE();
	STORAGE(Cerberus* p);
	~STORAGE();
	void free();
	string name();

	STORAGE(Cerberus* p, const char* pszDatabase, const char* pszTable);
	virtual bool load(const char* pszTable);
	bool reserve(int64 c);
	void saveFields();
	void setNumRows(int64 rows);
	FIELD* field(int32 i);
	FIELD* field(const char* pszField);
	FIELD* findField(const char* pszField);
	int64 findFieldIndex(const char* pszField);
	bool load(const char* pszDatabase, const char* pszTable);
	MEMORY* select(EXPRESSION_LIST &elSelect, EXPRESSION &where, EXPRESSION &group, EXPRESSION &order, int64 limit);
	MEMORY* join(TABLE* t1, EXPRESSION_LIST &elSelect, EXPRESSION &where, EXPRESSION &group, EXPRESSION &order, int64 limit);
	bool create(string database, string table, std::vector<FIELD> &fields);
	virtual bool create(string storage, std::vector<FIELD> &fields);
	bool setDatabase(const char* pszDatabase);
	FIELD* add_field(string name, FIELDTYPE field_type, int64 size, bool bFile);
	bool insert(STRINGS &fields, STRINGS &values);
	bool insert(STRINGS &values);
	bool insert(string s);
	void dump();
	void send(STREAM &stream);
	bool write(FIELD* f, int64 i, unsigned char* val, int64 ulSize, int field_i);

	virtual bool resize(int64 ulColumnId, int64 ulSize);
	STORAGE* search(EXPRESSION expression);
	int64 compileExpressionList(EXPRESSION_LIST &exprList, STRINGS& svTokens, int64 &i, TABLE** tables, int table_count);
	EXPRESSION compileExpression(STRINGS& svTokens, int64 &i, TABLE** tables, int table_count);
	EXPRESSION_RESULT resolve(EXPRESSION &where, int64 **ulRow, int64 &x, TABLE** tables, int table_i);
	EXPRESSION_RESULT evaluate(EXPRESSION &where, int64 &x, int64 **ulRow, TABLE** tables, int table_i);
};

#include "table.h"
#include "memory.h"

#endif

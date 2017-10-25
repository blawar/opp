#ifndef CERBERUS_H
#define CERBERUS_H

enum NULLTYPE
{
        is_null, is_not_null
};

enum FIELDTYPE
{
        t_none,
        t_byte,
        t_char,
        t_int,
        t_short,
        t_long,
        t_word,
        t_varchar,
        t_varbyte,
        t_text,
        t_binary,
        t_stream
};

#include <vector>
#include <iostream>
#include <fstream>
#include <string>
#include <stdio.h>
#include <string.h>
#include <fstream>
#include <boost/thread.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/regex.hpp>
#include <boost/lexical_cast.hpp>
#include "sx/libsx.h"
#include "opp/expression.h"
#include "sx/stream.h"
#include "opp/storage/storage.h"

//#define DEBUG_NETWORK
//#define STRINGS vector<string>

#define S(s) boost::lexical_cast<string>(s)

string SQL_BINARY(string s);

class TABLE;
class MEMORY;
class TBLOB;

class Cerberus
{
public:
	Cerberus();
	~Cerberus();
	MEMORY* query(const char* strQuery, int64 qlen);
	MEMORY* query(string query);
	void setDirectory(string path);
	bool create(const char* pszName);
	bool setdatabase(const char* pszName);
	bool runServer();
	bool client(int s);
	bool load_sql(const char* pszFile);

	TABLE* openTable(const char* pszTable);
	bool closeTable(const char* pszTable);
	bool tokenize(const char* pszStr, STRINGS &tokens);

	void error(string e);
	const char* error(char* pszError, STRINGS &tokens, int64 pos);

private:
	std::ofstream fout;
	string szCurrentDb;
	std::vector<TABLE*> pvTables;
};

#endif

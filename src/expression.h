#ifndef EXPRESSION_H
#define EXPRESSION_H

#include <vector>
#include <iostream>
#include <fstream>
#include <string>
#include <string.h>
#include <ctype.h>

void parseColumnString(string s, string &table, string &column, string &property, string &offset, string &length);
void parseColumnString(string s, string &table, string &column, string &property, string &offset);

class EXPRESSION
{
public:
	std::vector<unsigned char> expr;
	string alias;
	int64 size();
	void push_back(int8 c);
	void write_long(int64 v)
	{
		char* s = (char*)&v;
		for(int i=0; i < sizeof(v); i++) push_back(s[i]);
	}

	int64 read_long(int64 i)
	{
		return *(int64*)ptr(i);
	}

	int8* ptr(int64 i);
	int8 operator[](int64 i);
};

#define EXPRESSION_LIST std::vector<EXPRESSION>

struct EXPRESSION_RESULT
{
	int64 size;
	int64 value;
	unsigned char type;
};


#define OP_LT	0x01
#define OP_LTE	0x02
#define OP_GT	0x03
#define OP_GTE	0x04
#define OP_EQ	0x05
#define OP_NEQ	0x06
#define OP_ADD	0x07
#define OP_SUB	0x08
#define OP_MUL	0x09
#define OP_DIV	0x0A
#define OP_AND	0x0B
#define OP_OR	0x0C

#define OP_PAREN_OPEN	0xC0
#define OP_PAREN_CLOSE	0xC1

#define OP_INT		0xED
#define OP_CHAR		0xEF
#define OP_FIELD	0xF0
#define OP_LONG		0xF1
#define OP_SHORT	0xF2
#define OP_VARCHAR	0xF3
#define OP_STREAM	0xF4

#endif

#include "opp/cerberus.h"
#include "opp/storage/storage.h"
#include "opp/expression.h"

bool isNumeric(string szString)
{
	long len = szString.size();
	if (!len)
		return false;
	for (long i = 0; i < len; i++)
	{
		if (!isdigit(szString[i]))
			return false;
	}
	return true;
}

bool inVector(const char* s, STRINGS &svString)
{
	long len = svString.size();
	for (long i = 0; i < len; i++)
	{
		if (!svString.compare(i, s))
			return true;
	}
	return false;
}

void parseColumnString(string s, string &table, string &column, string &property, string &offset)
{
	table = "";
	column = "";
	property = "";
	offset = "";
	long len = s.size(), last_i=0;
	for(long i=0; i < len; i++)
	{
		if(s[i] == '.')
		{
			table = s.substr(last_i, i - last_i);
			last_i = i+1;
			continue;
		}

		if(s[i] == '[')
		{
			long x;
			column = s.substr(last_i, i - last_i);
			last_i = i + 1;
			for(x=last_i; x < len; x++)
			{
				if(s[x] == ']')
				{
					offset = s.substr(last_i, x - last_i);
					break;
				}
			}
			i = x;
			last_i = i + 1;
			continue;
		}

		if(s[i] == ':')
		{
			if(!column.size()) column = s.substr(last_i, i - last_i);
			last_i = i+1;
			property = s.substr(last_i);
			return;
		}
	}
	if(!column.size()) column = s.substr(last_i);
}

void parseColumnString(string s, string &table, string &column, string &property, string &offset, string &length)
{
	bool bOffset = false;
	table = "";
	column = "";
	property = "";
	offset = "";
	length = "";
	long len = s.size(), last_i=0;
	for(long i=0; i < len; i++)
	{
		if(s[i] == '.')
		{
			table = s.substr(last_i, i - last_i);
			last_i = i+1;
			continue;
		}

		if(s[i] == '[')
		{
			long x;
			column = s.substr(last_i, i - last_i);
			last_i = i + 1;
			for(x=last_i; x < len; x++)
			{
				if(s[x] == ']')
				{
					offset = s.substr(last_i, x - last_i);
					break;
				}
			}
			i = x;
			last_i = i + 1;
			bOffset = true;
			continue;
		}

		if(s[i] == ':')
		{
			if(!column.size()) column = s.substr(last_i, i - last_i);
			last_i = i+1;
			property = s.substr(last_i);
			return;
		}
		else if(bOffset)
		{
			length = s.substr(last_i);
			break;
		}
	}
	if(!column.size()) column = s.substr(last_i);
}

EXPRESSION_RESULT STORAGE::evaluate(EXPRESSION &where, int64 &x, int64 **ulRow, TABLE** tables, int table_i)
{
	int64 where_sz = where.size();
	EXPRESSION_RESULT erResult;
	for (; x < where_sz; x++)
	{
		unsigned char bOp = where[x];
		switch (where[x])
		{
		case OP_GT:
		case OP_GTE:
		case OP_LT:
		case OP_LTE:
		case OP_EQ:
		case OP_NEQ:
		case OP_AND:
		case OP_OR:
		case OP_ADD:
		case OP_SUB:
		case OP_MUL:
		case OP_DIV:
			{
				x++;
				EXPRESSION_RESULT b = resolve(where, ulRow, x, tables, table_i);
				switch (erResult.type)
				{
				case OP_INT:
					{
						switch (b.type)
						{
						case OP_LONG:
							switch (bOp)
							{
							case OP_GT:
								*(long*) &erResult.value = *(int*) &erResult.value > *(long*) &b.value;
								erResult.type = OP_LONG;
								break;
							case OP_LT:
								*(long*) &erResult.value = *(int*) &erResult.value < *(long*) &b.value;
								erResult.type = OP_LONG;
								break;
							case OP_EQ:
								*(long*) &erResult.value = *(int*) &erResult.value == *(long*) &b.value;
								erResult.type = OP_LONG;
								break;
							case OP_NEQ:
								*(long*) &erResult.value = *(int*) &erResult.value != *(long*) &b.value;
								erResult.type = OP_LONG;
								break;
							case OP_AND:
								//std::cout << "comparing " << *(int*) &erResult.value << " && " << *(long*) &b.value << " = ";
								*(long*) &erResult.value = *(int*) &erResult.value && *(long*) &b.value;
								erResult.type = OP_LONG;
								//std::cout  << *(long*) &erResult.value << std::endl;
								break;
							case OP_OR:
								*(long*) &erResult.value = *(int*) &erResult.value || *(long*) &b.value;
								erResult.type = OP_LONG;
								break;
							case OP_ADD:
								*(long*) &erResult.value = *(int*) &erResult.value + *(long*) &b.value;
							}
							break;
						case OP_INT:
							switch (bOp)
							{
							case OP_GT:
								*(int*) &erResult.value = *(int*) &erResult.value
									> *(int*) &b.value;
								erResult.type = OP_INT;
								break;
							case OP_LT:
								*(int*) &erResult.value = *(int*) &erResult.value
									< *(int*) &b.value;
								erResult.type = OP_INT;
								break;
							case OP_EQ:
								*(int*) &erResult.value = *(int*) &erResult.value
									== *(int*) &b.value;
								erResult.type = OP_INT;
								break;
							case OP_NEQ:
								*(int*) &erResult.value = *(int*) &erResult.value
									!= *(int*) &b.value;
								erResult.type = OP_INT;
								break;
							case OP_AND:
								*(int*) &erResult.value = *(int*) &erResult.value && *(int*) &b.value;
								erResult.type = OP_INT;
							case OP_OR:
								*(int*) &erResult.value = *(int*) &erResult.value || *(int*) &b.value;
								erResult.type = OP_INT;
								break;
							case OP_ADD:
								*(int*) &erResult.value = *(int*) &erResult.value + *(int*) &b.value;
							}
							break;
						default:
							std::cout << "Unhandled int op" << std::endl;
							break;
						}
						break;
					}
				case OP_LONG:
					{
						switch (b.type)
						{
						case OP_LONG:
							switch (bOp)
							{
							case OP_GT:
								*(long*) &erResult.value = *(long*) &erResult.value
									> *(long*) &b.value;
								erResult.type = OP_LONG;
								break;
							case OP_LT:
								*(long*) &erResult.value = *(long*) &erResult.value
									< *(long*) &b.value;
								erResult.type = OP_LONG;
								break;
							case OP_EQ:
								*(long*) &erResult.value = *(long*) &erResult.value
									== *(long*) &b.value;
								erResult.type = OP_LONG;
								break;
							case OP_NEQ:
								*(long*) &erResult.value = *(long*) &erResult.value
									!= *(long*) &b.value;
								erResult.type = OP_LONG;
								break;
							case OP_AND:
								*(long*) &erResult.value = *(long*) &erResult.value && *(long*) &b.value;
								erResult.type = OP_LONG;
								break;
							case OP_OR:
								*(long*) &erResult.value = *(long*) &erResult.value || *(long*) &b.value;
								erResult.type = OP_LONG;
								break;
							case OP_ADD:
								*(long*) &erResult.value = *(long*) &erResult.value + *(long*) &b.value;
								erResult.type = OP_LONG;
								break;
							}
							break;
						case OP_INT:
							switch (bOp)
							{
							case OP_GT:
								*(long*) &erResult.value = *(long*) &erResult.value
									> *(int*) &b.value;
								erResult.type = OP_LONG;
								break;
							case OP_LT:
								*(long*) &erResult.value = *(long*) &erResult.value
									< *(int*) &b.value;
								erResult.type = OP_LONG;
								break;
							case OP_EQ:
								*(long*) &erResult.value = *(long*) &erResult.value
									== *(int*) &b.value;
								erResult.type = OP_LONG;
								break;
							case OP_NEQ:
								*(long*) &erResult.value = *(long*) &erResult.value
									!= *(int*) &b.value;
								erResult.type = OP_LONG;
								break;
							case OP_AND:
								*(long*) &erResult.value = *(long*) &erResult.value && *(int*) &b.value;
								erResult.type = OP_LONG;
								break;
							case OP_OR:
								*(long*) &erResult.value = *(long*) &erResult.value || *(int*) &b.value;
								erResult.type = OP_LONG;
								break;
							case OP_ADD:
								*(long*) &erResult.value = *(long*) &erResult.value + *(int*) &b.value;
								erResult.type = OP_LONG;
								break;
							}
							break;
						default:
							printf("Unhandled long op: %x\n", b.type);
							break;
						}
						break;
					}
				default:
					std::cout << "unknown datatype" << std::endl;
					break;
				}
				break;
			}
		case OP_PAREN_OPEN:
			++x;
			erResult = evaluate(where, x, ulRow, tables, table_i);
			break;
		case OP_PAREN_CLOSE:
			return erResult;
			break;
		default:
			erResult = resolve(where, ulRow, x, tables, table_i);
			break;
		}
	}
	return erResult;
}

EXPRESSION_RESULT STORAGE::resolve(EXPRESSION &where, int64 **ulRow, int64 &x, TABLE** tables, int table_i)
{
	int64 length, offset;
	string* pbuffer = NULL;
	EXPRESSION_RESULT r;
	r.value = 0;
	r.type = 0;

	switch(where[x])
	{
	case OP_FIELD:
		{
			int64 t_i = x+sizeof(int8)+sizeof(int64)+sizeof(int64)+sizeof(int8);
			int64 primary_key = *ulRow[where[t_i]];
			TABLE* table = tables[where[t_i]];
			FIELD* field = table->pfields.at(where[t_i+1]);

			offset = where.read_long(x+1);
			length = where.read_long(x+1+sizeof(int64));
			x += 3 + sizeof(int64) + sizeof(int64);
			if(where[x-2] == 1)
			{
				r.type = OP_LONG;
				r.value = (int64)field->get_stream_size(primary_key);
			}
			else if(length)
			{
				r.type = OP_VARCHAR;
				pbuffer = new string();
				field->read_stream(primary_key, *pbuffer, offset, length);
				r.value = (int64)pbuffer;
				r.size = pbuffer->size();
			}
			else switch (field->type())
			{
			case t_int:
				{
					r.type = OP_INT;
					r.value = (int64)field->read_int(primary_key);
					break;
				}
			case t_byte:
			case t_char:
				{
					r.type = OP_CHAR;
					pbuffer = new string();
					r.size = field->size();
					field->read_bytes(primary_key, pbuffer);
					r.value = (int64)pbuffer;
				}
				break;
			case t_short:
				r.type = OP_SHORT;
				r.value = (int64)field->read_short(primary_key);
				break;
			case t_long:
				r.type = OP_LONG;
				r.value = (int64)field->read_long(primary_key);
				break;
			case t_varchar:
				{
					r.type = OP_VARCHAR;
					pbuffer = new string();
					field->read_blob(primary_key, *pbuffer);
					r.value = (int64)pbuffer;
					r.size = pbuffer->size();
				}
				break;
			case t_stream:
				{
					r.type = OP_VARCHAR;
					pbuffer = new string();
					field->read_stream(primary_key, *pbuffer);
					r.value = (int64)pbuffer;
					r.size = pbuffer->size();
				}
				break;

			default:
				std::cout << "unknown op 2" << std::endl;
			}
		}
		break;
	case OP_LONG:
		*(int64*)&r.value = *(int64*)(where.ptr(++x));
		r.type = OP_LONG;
		x += sizeof(int64) - 1;
		break;
	case OP_PAREN_OPEN:
		++x;
		return evaluate(where, x, ulRow, tables, table_i);
		break;
	default:
		printf("Unknown op: %x\n");
	}
	return r;
}

int64 STORAGE::compileExpressionList(EXPRESSION_LIST &exprList, STRINGS& svTokens, int64 &i, TABLE** tables, int table_count)
{
	int64 count = 0;
	while(i < svTokens.size() && svTokens.compare(i, "FROM"))
	{
		EXPRESSION expr = compileExpression(svTokens, i, tables, table_count);
		if (!svTokens.compare(i, "as"))
		{
			expr.alias = svTokens[i+1];
			i++;
		}
		else
		{
			expr.alias = svTokens[i-1];
		}

		if(!svTokens.compare(i, ",")) i++;

		exprList.push_back(expr);
		count++;
	}
	return count;
}

EXPRESSION STORAGE::compileExpression(STRINGS& svTokens, int64 &i, TABLE** tables, int table_count)
{
	string szTable, szProperty, szColumn, szOffset, szLength;
	EXPRESSION ucvResult;
	int64 len = svTokens.size();

	while (i < len)
	{
		if (!svTokens.compare(i, "("))
		{
			ucvResult.push_back(OP_PAREN_OPEN);
		}
		else if (!svTokens.compare(i, ")"))
		{
			ucvResult.push_back(OP_PAREN_CLOSE);
		}
		else if (!svTokens.compare(i, "and") || !svTokens.compare(i, "&&"))
		{
			ucvResult.push_back(OP_AND);
		}
		else if (!svTokens.compare(i, "or") || !svTokens.compare(i, "||"))
		{
			ucvResult.push_back(OP_OR);
		}
		else if (!svTokens.compare(i, ">"))
		{
			ucvResult.push_back(OP_GT);
		}
		else if (!svTokens.compare(i, ">="))
		{
			ucvResult.push_back(OP_GTE);
		}
		else if (!svTokens.compare(i, "<"))
		{
			ucvResult.push_back(OP_LT);
		}
		else if (!svTokens.compare(i, "<="))
		{
			ucvResult.push_back(OP_LTE);
		}
		else if (!svTokens.compare(i, "="))
		{
			ucvResult.push_back(OP_EQ);
		}
		else if (!svTokens.compare(i, "!="))
		{
			ucvResult.push_back(OP_NEQ);
		}
		else if (!svTokens.compare(i, "+"))
		{
			ucvResult.push_back(OP_ADD);
		}
		else if (!svTokens.compare(i, "+"))
		{
			ucvResult.push_back(OP_SUB);
		}
		else if (isNumeric(svTokens[i]))
		{
			int64 n = atol(svTokens[i]);
			ucvResult.push_back(OP_LONG);
			for (int x = 0; x < sizeof(int64); x++)
			{
				ucvResult.push_back(((char*) &n)[x]);
			}
		}
		else if (!svTokens.compare(i, "as"))
		{
			i += 2;
			break;
		}
		else if (!svTokens.compare(i, "group") || !svTokens.compare(i, "order") || !svTokens.compare(i, "limit") || !svTokens.compare(i, "from") || !svTokens.compare(i, ","))
		{
			break;
		}
		else // it is a string / field
		{
			long column = -1;
			long table = -1;
			long use_table = -1;

			parseColumnString(string(svTokens[i]), szTable, szColumn, szProperty, szOffset, szLength);

			int start=0, end=table_count;

			if(szTable.size())
			{
				for(int64 x=0; x < table_count; x++)
				{
					if(szTable == tables[x]->szName)
					{
						use_table = x;
						break;
					}
				}
			}

			for(int ts_i=start; ts_i < end; ts_i++)
			{
				unsigned char j = 0;
				/*if (!svTokens.length(i))
				{
					i++;
					continue;
				}*/
				for (; j < pfields.size(); j++)
				{
					if (szColumn == tables[ts_i]->pfields[j].name)
					{
						table = ts_i;
						column = j;
						break;
					}
				}
			}

			if(column == -1)
			{
				std::cout << "Could not find column: " << svTokens[i] << " FROM " <<  ", start: " << start << ", end: " << end << std::endl;
				throw "field not found";
			}

			ucvResult.push_back(OP_FIELD);
			if(szOffset.size()) ucvResult.write_long(boost::lexical_cast<int64>(szOffset));
			else ucvResult.write_long(0);

			if(szLength.size()) ucvResult.write_long(boost::lexical_cast<int64>(szLength));
			else ucvResult.write_long(0);

			if(szProperty == "length") ucvResult.push_back(1);
			else ucvResult.push_back(0);
			ucvResult.push_back(table);
			ucvResult.push_back(column);

			//ucvResult.push_back(svTokens[i].size());
			//for(long x=0; x < svTokens[i].size(); x++) ucvResult.push_back((svTokens[i])[x]);
		}
		i++;
	}

	/*	for(long j=0; j < ucvResult.size(); j++)
	{
	printf("%x\n", ucvResult[j]);
	}*/

	return ucvResult;
}

int64 EXPRESSION::size()
{
	return expr.size();
}

void EXPRESSION::push_back(int8 c)
{
	expr.push_back(c);
}

int8* EXPRESSION::ptr(int64 i)
{
	return (int8*)&expr[i];
}

int8 EXPRESSION::operator[](int64 i)
{
	return expr[i];
}

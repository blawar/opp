#include "opp/cerberus.h"
#include <boost/progress.hpp>
#include <fstream>

extern string DATA_DIR;

void FIELDS::save(string base)
{
	string file = base + "schema.oph", szData;
	std::ofstream f(file.c_str(), std::ios::binary);
	for(int i=0; i < size(); i++)
	{
		int16 len = (int16)at(i)->name.size(), type = (int16)at(i)->field_type;
		int64 size = at(i)->field_size;
		f.write((const char*)&len, sizeof(len));
		f << at(i)->name.c_str();
		f.write((const char*)&type, sizeof(type));
		f.write((const char*)&size, sizeof(size));
	}
	f.close();
}

void FIELDS::load(string base)
{
	string name, szData, file = base + "schema.oph";
	std::ifstream f(file.c_str(), std::ios::binary);

	if(f.is_open())
	{
		while(!f.eof())
		{
			FIELD* field = new FIELD();
			int16 len;
			int16 type;
			int64 size;
			f.read((char*)&len, sizeof(len));

			name.resize(len);
			f.read((char*)name.c_str(), len);
			f.read((char*)&type, sizeof(type));
			f.read((char*)&size, sizeof(size));

			field->field_type = (FIELDTYPE)type;
			field->field_size = size;
			field->name = name;

			field->load(base + field->name);

			push_back(field);
		}
		f.close();
	}
}

STORAGE::STORAGE()
{
}

STORAGE::STORAGE(Cerberus* cp)
{
	pcParent = cp;
	num_rows = ulRowSize = 0;
}

STORAGE::~STORAGE()
{
	free();
}

void STORAGE::free()
{
	for(long i = 1; i < pfields.size(); i++)
	{
		if(pfields.at(i)->pFile)
		{
			delete pfields.at(i)->pFile;
			pfields.at(i)->pFile = NULL;
		}

		if(pfields.at(i)->blob)
		{
			delete pfields.at(i)->blob;
			pfields.at(i)->blob = NULL;
		}
		delete pfields.at(i);
	}
	pfields.resize(0);
}

STORAGE::STORAGE(Cerberus* cp, const char* pszDatabase, const char* pszTable)
{
	pcParent = cp;
	num_rows = ulRowSize = 0;
	load(pszDatabase, pszTable);
}

string STORAGE::name()
{
	return szName;
}

STORAGE* STORAGE::search(EXPRESSION expression)
{
	return NULL;
}

bool STORAGE::setDatabase(const char* pszDatabase)
{
	szCurrentDb = pszDatabase;
	return true;
}

bool STORAGE::create(string database, string storage, std::vector<FIELD> &fields)
{
	setDatabase(database.c_str());
	return create(storage, fields);
}

void STORAGE::saveFields()
{
	string base = DATA_DIR + szCurrentDb + "/" + szName + "/";
	pfields.save(base);
}

FIELD* STORAGE::add_field(string name, FIELDTYPE field_type, int64 size, bool bFile)
{
	if(bFile)
	{
		string base = DATA_DIR + szCurrentDb + "/" + szName + "/";

		if(!boost::filesystem::exists(base.c_str())) boost::filesystem::create_directory(base.c_str());

		FIELD* field = new FIELD();
		int64 x = 0;
		field->field_type = field_type;
		field->field_size = size;
		field->name = name;

		field->load(base + field->name);
		pfields.push_back(field);
		return field;
	}
	else
	{
		FIELD* field = new FIELD();
		int64 x = 0;
		field->field_type = field_type;
		field->field_size = size;
		field->name = name;

		field->load("");
		pfields.push_back(field);
		return field;
	}
}

bool STORAGE::create(string table, std::vector<FIELD> &fields)
{
	szName = table;
	/*if(field_i)
	{
		string e = "table already exists: ";
		e = e + table;
		throw e.c_str();
	}*/
	string base = DATA_DIR + szCurrentDb + "/" + table + "/";
	string schema = base + "schema.oph";

	boost::filesystem::create_directory(base.c_str());


	for (int i = 0; i < fields.size(); i++)
	{
		add_field(fields[i].name, fields[i].field_type, fields[i].field_size, true);
	}

	pfields.save(schema);

	load(table.c_str());

	return true;
}

void STORAGE::dump()
{
/*	string str_buffer;
	TBLOB_ROW_TYPE size = 0;
	fout << num_rows << " rows" << std::endl;
	for(int64 i=0; i < field_i; i++)
	{
		if(*pfields.at(i)->name != '$') fout << pfields.at(i)->name << "\t";
	}
	fout << std::endl << "-----------------------------------------------------------" << std::endl;

	for(int64 i=0; i < num_rows; i++)
	{
		for(int64 j=0; j < field_i; j++)
		{
			if(*pfields[j].name == '$') continue;
			if(!j)
			{
				fout << (i+1) << "\t";
				continue;
			}
			FIELD* f = &(pfields[j]);
			switch(pfields[j].field_type)
			{
			case t_int:
				fout << read_int(i+1, j) << "\t";
				break;
			case t_long:
				fout << read_long(i+1, j) << "\t";
				break; 
			case t_varchar:
				fout << read_blob(i+1, j, str_buffer) << "\t";
				break;
			case t_byte:
			case t_char:
				{
					int64 size;
					fout << read_bytes(i+1, j, size, str_buffer) << "\t";
					break;
				}
			}
		}
		fout << std::endl;
	}
	*/
}

void STORAGE::send(STREAM &stream)
{
	string str_buffer;
	std::ostringstream sin;
	int8 bNumFields = (int8)pfields.size();
	stream.send_packet((char*)&bNumFields, 1);
	for(int64 i=0; i < pfields.size(); i++)
	{
		if(*pfields[i].name.c_str() == '$') continue;
		stream.write_begin();
		stream.write_string("std"); //catalog
		stream.write_string(szCurrentDb.c_str());
		stream.write_string(szName.c_str());
		stream.write_string(szName.c_str());
		stream.write_string(pfields.at(i)->name.c_str());
		stream.write_string(pfields.at(i)->name.c_str());
		stream.write_byte(0); // filler
		stream.write_binary("\x08\x00", 2); //charset number
		stream.write_binary("\x01\x00\x00\x00", 4); // length
		stream.write_byte(0xfe); // type ,string
		stream.write_binary("\x00\x00", 2); //flags
		stream.write_byte(0); // decimals
		stream.write_binary("\x00\x00", 2); // filler
		stream.write_send();
	}

	stream.send_packet("\xfe\x00\x00\x00\x00", 5);

	for(int64 i=0; i < num_rows; i++)
	{
		stream.write_begin();
		for(int64 j=0; j < pfields.size(); j++)
		{
			if(*pfields.at(i)->name.c_str() == '$') continue;
			if(!j)
			{
				sin << (i+1);
				stream.write_string(sin.str().c_str());
				sin.str("");
				continue;
			}
			FIELD* f = &(pfields[j]);
			switch(pfields[j].field_type)
			{
			case t_int:
				{
					sin << field(j)->read_int(i+1);
					stream.write_string(sin.str().c_str());
					sin.str("");
					break;
				}
			case t_long:
				{
					sin << field(j)->read_long(i+1);
					stream.write_string(sin.str().c_str());
					sin.str("");
					break;
				}
			case t_varchar:
				{
					TBLOB_ROW_TYPE ulSize;
					stream.write_string((char*)field(j)->read_blob(i+1, str_buffer), ulSize);
					break;
				}
			case t_byte:
			case t_char:
				{
					int64 ulSize;
					stream.write_string((char*)field(j)->read_bytes(i+1, &str_buffer), ulSize);
					break;
				}
			}
		}
		stream.write_send();
	}
	stream.send_packet("\xfe\x00\x00\x00\x00", 5);
}

MEMORY* STORAGE::select(EXPRESSION_LIST &elSelect, EXPRESSION &where, EXPRESSION &group, EXPRESSION &order, int64 limit)
{
	TABLE* tables = (TABLE*)this;
	MEMORY* result = new MEMORY(pcParent);
	STRINGS svReferencedFields;

	int64 x;

	int64 where_sz = where.size(), i=1;
	int64* primary_keys[] = {&i};
	EXPRESSION_RESULT erResult;

	for (; i <= num_rows; i++)
	{
		if(!field((int32)0)->read_bit(i)) continue;
		x = 0;
		EXPRESSION_RESULT erResult;
		if(where_sz)  erResult = evaluate(where, x, primary_keys, &tables, 1);
		if (!where_sz || *(int64*)&erResult.value)
		{
			int64 primary_key = ++result->num_rows;
			if(limit && primary_key > limit)
			{
				result->num_rows--;
				i = num_rows + 1;
				break;
			}
			for (int j = 0; j < elSelect.size(); j++)
			{
				int64 where_i = 0;
				EXPRESSION_RESULT r = evaluate(elSelect[j], where_i, primary_keys, &tables, 1);
				switch(r.type)
				{
				case OP_CHAR:
					{
						if(primary_key == 1)
						{
							result->add_field(elSelect[j].alias.c_str(), t_char, r.size);
						}

						string* s = (string*)r.value;
						result->field(j+1)->write_byte(primary_key, (int8*)s->c_str());
						delete s;
					}
					break;
				case OP_SHORT:
					if(primary_key == 1)
					{
						result->add_field(elSelect[j].alias.c_str(), t_short, sizeof(int16));
					}
					result->field(j+1)->write_short(primary_key, (int16_t)r.value);
					break;
				case OP_INT:
					if(primary_key == 1)
					{
						result->add_field(elSelect[j].alias.c_str(), t_int, sizeof(int32));
					}
					result->field(j+1)->write_int(primary_key, (int)r.value);
					break;
				case OP_LONG:
					if(primary_key == 1)
					{
						result->add_field(elSelect[j].alias.c_str(), t_long, sizeof(int64));
					}
					result->field(j+1)->write_long(primary_key, (int64)r.value);
					break;
				case OP_VARCHAR:
					{
						if(primary_key == 1)
						{
							result->add_field(elSelect[j].alias.c_str(), t_varchar, sizeof(int64));
						}
						string* s = (string*)r.value;
						result->field(j+1)->write_blob(primary_key, (int8*)s->c_str(), s->size());
						delete s;
					}
					break;
				default:
					throw "Unknown field value";
				}
			}
		}
	}

	return result;
}

MEMORY* STORAGE::join(TABLE* t2, EXPRESSION_LIST &elSelect, EXPRESSION &where, EXPRESSION &group, EXPRESSION &order, int64 limit)
{
	TABLE* tables[2] = {(TABLE*)this, t2};
	MEMORY* result = new MEMORY(pcParent);
	STRINGS svReferencedFields;

	int64 x;

	int64 where_sz = where.size(), i=1, i2=1;
	int64* primary_keys[] = {&i, &i2};

	EXPRESSION_RESULT erResult;

	//std::cout << "limit " << limit << std::endl;
	for (i=1; i <= tables[0]->num_rows; i++)
	{
		if(limit && result->num_rows > limit) break;
		//std::cout << "loop " << i << std::endl;
		for (i2=1; i2 <= tables[1]->num_rows; i2++)
		{
			x = 0;
			EXPRESSION_RESULT erResult;
			if(where_sz)  erResult = evaluate(where, x, primary_keys, tables, 2);
			if (!where_sz || *(int64*) &erResult.value)
			{
				int64 primary_key = ++result->num_rows;
				if(limit && (primary_key > limit))
				{
					i = tables[0]->num_rows + 1;
					i2 = tables[1]->num_rows + 1;
					result->num_rows--;
					break;
				}
				for (int j = 0; j < elSelect.size(); j++)
				{
					int64 where_i = 0;
					EXPRESSION_RESULT r = evaluate(elSelect[j], where_i, primary_keys, tables, 2);
					switch(r.type)
					{
						case OP_CHAR:
							{
								if(primary_key == 1)
								{
									result->add_field(elSelect[j].alias.c_str(), t_char, r.size);
								}

								string *s = (string*)r.value;
								result->field(j+1)->write_byte(primary_key, (int8*)s->c_str());
								delete s;
							}
							break;
						case OP_SHORT:
								if(primary_key == 1)
								{
									result->add_field(elSelect[j].alias.c_str(), t_short, sizeof(int16));
								}
							result->field(j+1)->write_short(primary_key, (int16_t)r.value);
							break;
						case OP_INT:
								if(primary_key == 1)
								{
									result->add_field(elSelect[j].alias.c_str(), t_int, sizeof(int32));
								}
							result->field(j+1)->write_int(primary_key, (int)r.value);
							break;
						case OP_LONG:
								if(primary_key == 1)
								{
									result->add_field(elSelect[j].alias.c_str(), t_long, sizeof(int64));
								}
							result->field(j+1)->write_long(primary_key, (int64)r.value);
							break;
						case OP_VARCHAR:
							{
								if(primary_key == 1)
								{
									result->add_field(elSelect[j].alias.c_str(), t_varchar, sizeof(int64));
								}
								string *s = (string*)r.value;
								result->field(j+1)->write_blob(primary_key, (int8*)s->c_str(), r.size);
								delete s;
							}
							break;
						default:
							throw "Unknown field value";
					}
				}
			}
		}
	}

	return result;
}

bool STORAGE::load(const char* pszDatabase, const char* pszTable)
{
	setDatabase(pszDatabase);
	return load(pszTable);
}

bool STORAGE::load(const char* pszTable)
{
	szName = pszTable;
	string base = DATA_DIR + szCurrentDb + "/" + pszTable + "/";
	pfields.load(base);
	if(pfields.size())
	{
		pfields[0].pFile->readbuf(0, &num_rows, sizeof(num_rows));
	}
	return true;
}

void STORAGE::setNumRows(int64 rows)
{
	if(rows > num_rows && pfields.size())
	{
		num_rows = rows;
		pfields[0].pFile->readbuf(0, &num_rows, sizeof(num_rows));
	}
}

FIELD* STORAGE::field(int32 i)
{
	return pfields.at(i);
}

FIELD* STORAGE::field(const char* pszField)
{
	string sField = pszField;
	for (int64 i = 0; i < pfields.size(); i++)
	{
		if (!sField.compare(pfields.at(i)->name))
		{
			return pfields.at(i);
		}
	}
	sField = "unable to locate column: ";
	sField = sField + pszField;
	throw sField.c_str();
	return NULL;
}

FIELD* STORAGE::findField(const char* pszField)
{
	return field(pszField);
}

int64 STORAGE::findFieldIndex(const char* pszField)
{
        string sField = pszField;
        for (int64 i = 0; i < pfields.size(); i++)
        {
                if (!sField.compare(pfields.at(i)->name))
                {
                        return i;
                }
        }
        sField = "unable to locate column: ";
        sField = sField + pszField;
        throw sField.c_str(); 
        return -1;
}

unsigned char STORAGE::fieldSize(FIELD &f)
{
	return fieldSize(&f);
}

unsigned char STORAGE::fieldSize(FIELD *f)
{
	switch (f->field_type)
	{
	case t_char:
	case t_byte:
		return f->field_size;
	case t_int:
		return sizeof(int32);
	case t_word:
		return sizeof(int32);
	case t_long:
	default:
		return sizeof(int64);
	}
}

bool STORAGE::reserve(int64 c)
{
	std::cout << "reserving " << c << std::endl;
	for(int64 i=1; i < pfields.size(); i++)
	{
		resize(i, fieldSize(pfields[i]) * c);
	}
	return true;
}

bool STORAGE::resize(int64 ulColumnId, int64 ulSize)
{
	return pfields[ulColumnId].pFile->realloc(ulSize);
}

bool STORAGE::write(FIELD* f, int64 i, unsigned char* val, int64 ulSize, int field_i)
{
	if(!field_i) return false;
	int64 offset;
	switch (f->field_type)
	{
	case t_byte:
	case t_char:
		field(field_i)->write_byte(i, val);
		break;
	case t_short:
		field(field_i)->write_short(i, atoi((char*) val));
		break;
	case t_int:
		field(field_i)->write_int(i, atoi((char*) val));
		break;
	case t_long:
		field(field_i)->write_long(i, atol((char*) val));
		break;
	case t_varchar:
	{
		field(field_i)->write_blob(i, val, ulSize);
	}
	break;
	case t_stream:
	{
		field(field_i)->write_stream(i, val, ulSize, 0);
	}
	break;
	default:
		std::cout << "Unknown field " << f->field_type << ", " << f->name << std::endl;
		throw "Unknown field";
	}
	return true;
}

bool STORAGE::insert(STRINGS &values)
{
	STRINGS r;
	return insert(r, values);
}

bool STORAGE::insert(string s)
{
	STRINGS strings, fields;
	strings.push_back((string)"0");
	strings.push_back(s);

	for (int64 i = 0; i < pfields.size() && i < 2; i++)
	{
		fields.push_back(pfields[i].name.c_str());
	}
	return insert(fields, strings);
}

bool STORAGE::insert(STRINGS &fields, STRINGS &values)
{
	if (!fields.size())
	{
		for (int64 i = 0; i < pfields.size(); i++)
		{
			fields.push_back(pfields[i].name.c_str());
		}
	}

	if (fields.size() != values.size())
	{
                //fout << "different values: " << fields.size() << ", " << values.size() << std::endl;
                for (int i = 0; i < fields.size() || i < values.size(); i++)
                {
                        if (i >= fields.size())
                        {
                                //fout << "unknown = " << values[i] << std::endl;
                        }
                        else if (i >= values.size())
                        {
                                //fout << fields[i] << " = unknown" << std::endl;
                        }
                        else
                        {
                                //fout << fields[i] << " = " << values[i] << std::endl;
                        }
                }
				//error("columns and values have different amounts");
		throw "columns and values have different amounts";
		return false;
	}

	int64 primary_key = 0;
	int64 len = fields.size();

	for (int64 j = 0; j < len; j++)
	{
		if (!fields.compare(j, pfields[0].name))
		{
			primary_key = strtoul(values[j], NULL, 0);
			break;
		}
	}

	++num_rows;
	if (!primary_key)
		primary_key = num_rows;

	for (int64 j = 0; j < pfields.size(); j++)
	{
		string table, column, property, offset;
		bool bFound = false;
		for (int64 i = 1; i < len; i++)
		{
			parseColumnString(string(fields[i]), table, column, property, offset);
			if (column == pfields[j].name)
			{
				//error(S("column: ") + column + "  offset: " + offset + "  property: " + property);
				bFound = true;
				if (!primary_key)
				{
					primary_key = strtoul(values[i], NULL, 0);
					if (!primary_key)
						primary_key = 1;
					break;
				}

				if(offset.size())
				{
					//error(S("offset: ") + offset);
					field(j)->write_stream(primary_key, (int8*)values[i], values.length(i), atol(offset.c_str()));
				}
				else if(!property.size())
				{
					unsigned char field_size = fieldSize(pfields[j]);
					int64 pos = (primary_key - 1) * field_size;

					resize(j, pos + field_size);
					write(&pfields[j], primary_key, (unsigned char*) values[i], values.length(i), j);
				}
				else
				{
					int32 v = atol(values[i]);
					field(j)->create_stream(primary_key, v);
				}

				//break;
			}
			else
			{
			}
		}
		if (!bFound)
		{
			if (!primary_key)
				primary_key = 1;
		}
	}
	if(primary_key > num_rows)
	{
		num_rows = primary_key;
		field((int32)0)->write_long(1, num_rows);
	}
	field((int32)0)->write_bit(primary_key, true);
	return false;
}

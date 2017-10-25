#include "opp/cerberus.h"
#include "opp/storage/storage.h"
#include "opp/expression.h"


string DATA_DIR = "./";

string SQL_BINARY(string s)
{
	string r;
	r.reserve(s.size() + 10);
	r = S(s.size()) + "#" + s + "#";
	//r.append(s.c_str(), s.size());
	//r += "#";
	return r;
}

void Cerberus::error(string e)
{
	fout << e << std::endl;
}

Cerberus::Cerberus()
{
	fout.open("error.log", std::ios::trunc);
}

void Cerberus::setDirectory(string path)
{
	DATA_DIR = path + "/";
}

Cerberus::~Cerberus()
{
	fout << "Shutting down" << std::endl;
	for (int64 i = 0; i < pvTables.size(); i++)
	{
		//pvTables[i]->~TABLE();
		delete pvTables[i];
	}
	pvTables.clear();
	fout.close();
}

TABLE* Cerberus::openTable(const char* pszTable)
{
	for (int64 i = 0; i < pvTables.size(); i++)
	{
		if (!(pvTables[i]->name().compare(pszTable)))
		{
			//fout << "reopening table: " << pszTable << std::endl;
			return pvTables[i];
		}
	}
	fout << "opening table: " << pszTable << std::endl;
	TABLE* pTable = new TABLE(this, szCurrentDb.c_str(), pszTable);

	if (!pTable)
	{
		fout << "failed to open table: " << pszTable << std::endl;
		return NULL;
	}

	//pTable->load(pszTable);

	pvTables.push_back(pTable);
	return pTable;
}

bool Cerberus::closeTable(const char* pszTable)
{
	return false;
}

string err;

const char* Cerberus::error(char* pszError, STRINGS &tokens, int64 pos)
{
	err = pszError;
	err += "\n\n";
	int64 distance = 30, start, end;

	if (distance > pos)
	{
		start = 0;
		end = pos;
	}
	else if (pos + distance > tokens.size())
	{
		start = tokens.size() - distance;
		end = tokens.size();
	}
	else
	{
		start = pos - (distance / 2);
		end = pos + (distance / 2);
	}

	while (start < end)
	{
		if (start == pos)
			err += (string)"**" + tokens[start] + "** ";
		else
			err += (string)tokens[start] + " ";
		start++;
	}
	return err.c_str();
}

bool skip_sql_params(STRINGS &tokens, int64 &pos)
{
	int open=0;
	int64 len = tokens.size();
	while(pos < len && open >= 0)
	{
		if(!tokens.compare(pos, "(")) open++;
		else if(!tokens.compare(pos, ")")) open--;
		else if(!open && !tokens.compare(pos, ","))
		{
			pos--;
			return true;
		}
		pos++;
	}
	return false;
}

MEMORY* Cerberus::query(string query)
{
	return this->query(query.c_str(), query.size());
}

MEMORY* Cerberus::query(const char* pszQuery, int64 qlen)
{
	int64 ulQueryBase=0, i=0;
	STRINGS tokens;
	try
	{
		try
		{
			int64 limit = 0;
			EXPRESSION where, order, group;
			MEMORY *result = NULL;
			if(!qlen) qlen = strlen(pszQuery);
			tokens.reserve(qlen);
			tokenize(pszQuery, tokens);

			for(ulQueryBase = 0; ulQueryBase < tokens.size(); ulQueryBase++)
			{

				if (!tokens.compare(ulQueryBase, "select"))
				{
					TABLE* ts[255];
					int ts_i = 0;
					EXPRESSION_LIST elSelect;
					i = ulQueryBase + 1;
					int64 ulTable = 0;
					for (; i < tokens.size(); i++)
					{
						if (!tokens.compare(i, "from"))
						{
							break;
						}
					}

					if(!tokens.compare(i, "from"))
					{
						i++;
						ulTable = i++;
						ts[ts_i++] = openTable(tokens[ulTable]);
					}

					if(!tokens.compare(i, "inner"))
					{
						i++;
						if(tokens.compare(i++, "join"))
						{
							throw "expected 'join' keyword";
						}
						ulTable = i++;
						ts[ts_i++] = openTable(tokens[ulTable]);
					}


					if(ts_i)
					{
						if(i < tokens.size() && !tokens.compare(i, "where"))
						{
							i++;
							where = ts[0]->compileExpression(tokens, i, ts, ts_i);
						}

						if(i < tokens.size() && !tokens.compare(i, "group"))
						{
							i++;
							group = ts[0]->compileExpression(tokens, i, ts, ts_i);
						}

						if(i < tokens.size() && !tokens.compare(i, "order"))
						{
							i++;
							order = ts[0]->compileExpression(tokens, i, ts, ts_i);
						}

						if(i < tokens.size() && !tokens.compare(i, "limit"))
						{
							i++;
							limit = atol(tokens[i++]);
						}
                                        	i = ulQueryBase+1;
						//fout << "Done compiling where\n";
                                        	ts[0]->compileExpressionList(elSelect, tokens, i, ts, ts_i);
					}
					else
					{
						throw "no from";
					}


					if(result)
					{
						delete result;
						result = NULL;
					}
					if(ts_i == 1) result = ts[0]->select(elSelect, where, group, order, limit);
					if(ts_i == 2) result = ts[0]->join(ts[1], elSelect, where, group, order, limit);
				}
				else if (!tokens.compare(ulQueryBase, "update"))
				{
				}
				else if (!tokens.compare(ulQueryBase, "insert"))
				{
					int64 c = 0;
					int64 pos = 4;
					string szTable = tokens[ulQueryBase];
					STRINGS fields;
					STRINGS values;

					TABLE* t = openTable(tokens[ulQueryBase + 2]);
					//error(error("Hmm", tokens, ulQueryBase));

					if (!tokens.compare(ulQueryBase + 3, "set"))
					{

						for (i = 4 + ulQueryBase; i < tokens.size(); i++)
						{
							if (!tokens.compare(i, ","))
								continue;
							if (!tokens.compare(i, ";")) break;
							switch (c++ % 3)
							{
							case 0:
								if(!tokens.compare(i+1, ":"))
								{
									fields.push_back(S(tokens[i]) + ":" + tokens[i+2], string_field_compound);
									i+=2;
									//c+=2;
								}
								else if(!tokens.compare(i+1, "["))
								{
									fields.push_back(S(tokens[i]) + "[" + tokens[i+2] + "]", string_field_array);
									i+=3;
									//c+=2;
								}
								else
								{
									fields.push_back(tokens[i]);
								}
								break;
							case 1:
								if(tokens.compare(i, "="))
								{
									fout << "unknown operator: '" << tokens[i] << "', expected '='" << std::endl;
								}
								break;
							case 2:
								values.push_back(tokens[i], tokens.length(i));
								break;
							}
						}

						t->insert(fields, values);
					}
					else
					{
						i = 3 + ulQueryBase;
						if (!tokens.compare(i, "("))
						{

							for (++i; i < tokens.size(); i++)
							{
								if (!tokens.compare(i, ","))
									continue;
								if (!tokens.compare(i, ")"))
								{
									i++;
									break;
								}
								fields.push_back(tokens[i]);
							}
						}
						if(tokens.compare(i, "values"))
						{
							throw error("invalid insert syntax, expected VALUES", tokens, i);
						}
						i++;

						for (i++; i < tokens.size(); i++)
						{
							if (!tokens.compare(i, ","))
								continue;
							if (!tokens.compare(i, ";"))
								break;
							if (!tokens.compare(i, ")"))
							{
								//fout << error("hmmmm", tokens, i) << std::endl;
								//error(string("Inserting into ") + t->szName + " @" + values[0]);
								t->insert(fields, values);
								values.clear();
								if (!tokens.compare(++i, ";"))
								{
									ulQueryBase = i;
									break;
								}
								++i;
								continue;
							}
							values.push_back(tokens[i]);
						}

					}
				}
				else if (!tokens.compare(ulQueryBase, "create"))
				{
					if(!tokens.compare(ulQueryBase + 1, "table"))
					{
						int64 pos = ulQueryBase + 4;
						int64 opens = 1;
						FIELD field;

						if (tokens.compare(ulQueryBase + 3, "("))
							throw (const char*) "Unexpected character reached during create statement";

						TABLE* t = openTable(tokens[ulQueryBase + 2]);

						int sz = t->pfields.size();

						if(sz != 0)
						{
							throw "table already exists";
						}

						while (opens > 0 && pos < tokens.size())
						{
							if (!tokens.compare(pos, ")"))
								opens--;
							else if (!tokens.compare(pos, "("))
								opens++;

							if (!tokens.compare(pos, ","))
							{
								if(field.field_type)
								{
									t->add_field(field.name, field.field_type, field.field_size, true);
								}
								field.name = "";
								field.field_type = t_none;
								field.field_size = 0;
							}
							else if (!*field.name.c_str())
							{
								int64 len = tokens.length(pos);
								if (len < 0)
									len = 0;
								field.name = tokens[pos];
								//fout << "Field size: " << len << ", " << field.name << std::endl;
							}
							else if (!tokens.compare(pos, "long") || !tokens.compare(pos, "bigint"))
							{
								field.field_type = t_long;
								field.field_size = sizeof(int64);
								skip_sql_params(tokens, pos);
							}
							else if (!tokens.compare(pos, "int") || !tokens.compare(pos, "mediumint"))
							{
								field.field_type = t_int;
								field.field_size = sizeof(int32);
								skip_sql_params(tokens, pos);
							}
                            else if (!tokens.compare(pos, "short") || !tokens.compare(pos, "smallint"))
                            {
                                    field.field_type = t_int;
                                    field.field_size = sizeof(int16);
									skip_sql_params(tokens, pos);
                            }
							else if (!tokens.compare(pos, "varchar") || !tokens.compare(pos, "text"))
							{
								field.field_type = t_varchar;
								field.field_size = sizeof(int64);
								skip_sql_params(tokens, pos);
							}
							else if (!tokens.compare(pos, "char") || !tokens.compare(pos, "tinyint"))
							{
								if (!tokens.compare(pos+1, "(") && !tokens.compare(pos+3, ")"))
								{
									field.field_size = atoi(tokens[pos+2]);
									pos += 3;
								}
								else field.field_size = 1;
								field.field_type = t_char;
								skip_sql_params(tokens, pos);
							}
							else if (!tokens.compare(pos, "enum"))
							{
								/*if (tokens.compare(++pos, "(")) throw "expected parameters after enum";
								while(tokens.compare(pos++, ")"))
								{
								}*/
								skip_sql_params(tokens, pos);
								field.field_type = t_varchar;
							}
							else if (!tokens.compare(pos, "stream"))
							{
								field.field_type = t_stream;
								field.field_size = sizeof(int64);
								skip_sql_params(tokens, pos);
							}
							else // unknown data type
							{
								//printf("unknown datatype: %s\n", tokens[pos]);
								//memset(&field, 0, sizeof(field));
								//skip_sql_params(tokens, pos);
							}
							pos++;
						}
						if(field.field_type)
						{
							t->add_field(field.name, field.field_type, field.field_size, true);
						}

						long auto_increment = 0;
						while (pos < tokens.size())
						{
							if (!tokens.compare(pos, ";")) break;
							if (!tokens.compare(pos, "auto_increment"))
							{
								if(tokens.compare(pos+1, "=")) throw "expected = after auto_increment";
								auto_increment = atol(tokens[pos+2]);
								pos += 2;
							}
							pos++;
						}

						if(auto_increment) t->reserve(auto_increment);
						t->saveFields();
					}
					else if(!tokens.compare(ulQueryBase + 1, "database"))
					{
						create(tokens[ulQueryBase + 2]);
					}
				}
				else if (!tokens.compare(ulQueryBase, "show"))
				{
					if(!tokens.compare(ulQueryBase+1, "databases"))
					{
						FIELD f;
						if(result) delete result;
						result = new MEMORY(this);
						if(!result) throw "Failed to allocate memory for result table";


						result->add_field("num", t_varchar, sizeof(int64));
						result->add_field("Database", t_varchar, sizeof(int64));

						fs::path full_path = fs::system_complete( fs::path(DATA_DIR.c_str(), fs::native ) );
						fs::directory_iterator end_iter;

						for (fs::directory_iterator dir_itr(full_path); dir_itr != end_iter; ++dir_itr)
						{
							if (fs::is_directory(*dir_itr)) result->insert(dir_itr->leaf());
						}
					}
				}
			}

			return result;
		} catch (string caught)
		{
			fout << "An error has occurred: " << caught << std::endl ;//<< error((char*)caught, tokens, i) << std::endl;
			return false;
		}
	} catch (const char* caught)
	{
		fout << "An error has occurred: " << caught << std::endl;// << error((char*)caught, tokens, i) << std::endl;
		return false;
	}
}

bool Cerberus::create(const char* pszName)
{
	string szCreateDir = DATA_DIR + pszName;
	boost::filesystem::create_directory(szCreateDir.c_str());
	return false;
}

bool Cerberus::setdatabase(const char* pszName)
{
	szCurrentDb = pszName;
	return false;
}

bool Cerberus::tokenize(const char* pszStr, STRINGS &tokens)
{
	string token;

	char delimiters[3][2] =
	{
	{ '"', '"' },
	{ '\'', '\'' },
	{ '`', '`' } };
	int delimiter_active = -1;

	for (long i = 0; pszStr[i]; i++)
	{
		if (delimiter_active != -1)
		{
			if(pszStr[i] == '\\')
			{
				token += pszStr[++i];
				continue;
			}
			else if (pszStr[i] == delimiters[delimiter_active][1])
			{
				if(pszStr[i+1] != delimiters[delimiter_active][1])
				{
					switch(delimiter_active)
					{
					case '`':
						tokens.push_back(token, string_field);
						break;
					case '"':
						tokens.push_back(token, string_text);
						break;
					default:
						tokens.push_back(token);
					}
					token = "";
					delimiter_active = -1;
					continue;
				}
				i++;
				
			}
			token += pszStr[i];
			continue;
		}

		for (int d = 0; d < 3; d++)
		{
			if (pszStr[i] == delimiters[d][0])
			{
				delimiter_active = d;
				break;
			}
		}

		if (delimiter_active != -1)
			continue;

		if(pszStr[i] == '-' && pszStr[i+1] == '-' )
		{
			while(pszStr[i] && pszStr[i] != '\n') i++;
			continue;
		}

		if(pszStr[i] == '/' && pszStr[i+1] == '*' )
		{
			while(pszStr[i] && !(pszStr[i] == '*' && pszStr[i+1] == '/')) i++;
			if(pszStr[i]) i++;
			continue;
		}
		switch (pszStr[i])
		{
		case ' ':
		case '\t':
		case '\n':
		case '\r':
			if (token.length())
			{
				if (token.length())
				{
					tokens.push_back(token);
				}
				token = "";
			}
			break;
		case ';':
		case '=':
		case '+':
		case '-':
		case '/':
		case '*':
		case '!':
		case ',':
		case '(':
		case ')':
		//case '.':
			if (token.length())
			{
				tokens.push_back(token);
			}
			token = pszStr[i];
			tokens.push_back(token);
			token = "";
			break;
		case '#':
			{
				int64 nLength = boost::lexical_cast<int64>(token);

				token.resize(nLength);
				i++;
				if(pszStr[i + nLength] != '#')
				{
					throw string("Improper binary encoding");
				}
				memcpy((void*)token.c_str(), (void*)(pszStr+i), nLength);
				tokens.push_back(token);
				token = "";

				i += nLength;
			}
			break;
		default:
			token += tolower(pszStr[i]);
			break;
		}
	}
	if(token.size()) tokens.push_back(token);

	//error(string("Tokens: ") + boost::lexical_cast<string>(tokens.size()));

	/*for(long i=0; i < tokens.size(); i++)
	{
		error(tokens[i]);
	}*/
	return true;
}

bool Cerberus::client(int s)
{
	try
	{
		STREAM stream(s);
		int r = 0;
		stream.write_begin();
		stream.write_byte(0x0A); // version
		stream.write_binary("OPP v1", strlen("OPP v1") + 1); // string version

		stream.write_binary("\x00\x00\x00\x00", 4); // thread id
		stream.write_binary("\x69\x36\x6f\x4d\x74\x21\x45\x75", 8); // scramble
		stream.write_byte(0); // filler
		stream.write_binary("\x2c\xa2", 2); // server capabilities
		stream.write_byte(8); // server language
		stream.write_binary("\x20\x00", 2); // server status
		stream.write_binary("\x00000000000000000000000000", 13); // filler scramble
		stream.write_binary("\x00\x67\x31\x25\x61\x4c\x34\x72\x43\x7e\x53\x75\x3e", 13); // scramble
		stream.write_send();

		fout << "accepted connection " << std::endl;

		stream.load_next_packet();

		stream.send_ok();
		string str;

		while (stream.load_next_packet())
		{
			fout << "processing...\n";
			int8 bCmd = stream.read_byte();
			switch (bCmd)
			{
			case 0x3:
			{
				stream.read_str(str);
				fout << "Quering: " << str << std::endl;

				MEMORY* result = query((char*)str.c_str(), str.size());
				if(result)
				{
					result->send(stream);
					delete result;
					result = NULL;
					break;
				}
				else stream.send_ok();
				break;
			}
			case 1: // quit
				fout << "quit msg recvd" << std::endl;
				//close(s);
				return false;
				break;
			default:
				printf("Unknow command: %x\n", bCmd);
				//mysql_send_ok(s);
				//close(s);
				//return false;
			}
		}

		fout << "closing thread" << std::endl;

		//close(s);
		return false;
	} catch (const char* caught)
	{
		fout << "An error was encountered: " << caught << std::endl;
		return false;
	}
}

bool Cerberus::runServer()
{
	int sockfd, newsockfd, nPort = 1337, clilen, i = 0;
	sockaddr_in serv_addr, cli_addr;

	memset((void*)&serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(nPort);
	serv_addr.sin_addr.s_addr = INADDR_ANY;

	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0)
		error("ERROR opening socket");

	if (::bind(sockfd, (struct sockaddr*) &serv_addr, sizeof(serv_addr)) < 0)
		error("ERROR on binding");

	while (i++ < 5)
	{
		listen(sockfd, 5);

		clilen = sizeof(cli_addr);
		newsockfd = accept(sockfd, (struct sockaddr*) &cli_addr,
				(socklen_t*) &clilen);
		if (newsockfd < 0)
		{
			error("ERROR on accept");
			continue;
		}

		client(newsockfd);
	}

	return false;
}

bool Cerberus::load_sql(const char* pszFile)
{
        FILE* pFile = fopen(pszFile, "r");
        fseek(pFile, 0, SEEK_END);
        int64 size = ftell(pFile);

        char* buf = (char*) malloc(size);

        if (!buf)
        {
			error(string("Failed to load"));
            fclose(pFile);
            return 0;
        }

        rewind(pFile);
        fread(buf, 1, size, pFile);

        query(buf, size);

        free(buf);
        fclose(pFile);
		return false;
}

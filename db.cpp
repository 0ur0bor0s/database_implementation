/************************************************************
	Project#1:	CLP & DDL
 ************************************************************/

#include "db.h"
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>

#if defined(_WIN32) || defined(_WIN64)
#define strcasecmp _stricmp
#endif

int main(int argc, char** argv)
{
	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;

	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"\n");
		return 1;
	}

	rc = initialize_tpd_list();

    if (rc)
    {
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
    }
	else
	{
    	// Get token
    	rc = get_token(argv[1], &tok_list);

		/* Test code */
		/*tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class,
				      tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}*/
    
		if (!rc)
		{
			rc = do_semantic(tok_list);
		}
		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					  (tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}

    /* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
           tmp_tok_ptr = tok_ptr->next;
           free(tok_ptr);
           tok_ptr=tmp_tok_ptr;
		}
	}

	return rc;
}

/************************************************************* 
	This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc=0,i,j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;
	
	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
	  memset ((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank, (, ), or a comma, then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((strcasecmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
				  if (KEYWORD_OFFSET+j < K_CREATE)
						t_class = type_name;
				  else if (KEYWORD_OFFSET+j >= F_SUM)
            			t_class = function_name;
          		else
					  t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
					  add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do 
			{
				temp_string[i++] = *cur++;
			}
			while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank or a ), then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
		         || (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
				case '(' : t_value = S_LEFT_PAREN; break;
				case ')' : t_value = S_RIGHT_PAREN; break;
				case ',' : t_value = S_COMMA; break;
				case '*' : t_value = S_STAR; break;
				case '=' : t_value = S_EQUAL; break;
				case '<' : t_value = S_LESS; break;
				case '>' : t_value = S_GREATER; break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
    else if (*cur == '\'')
    {
      /* Find STRING_LITERRAL */
			int t_class;
      cur++;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((*cur) && (*cur != '\''));

      temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
      else /* must be a ' */
      {
        add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
        cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
        }
      }
    }
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}
			
  return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

    if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
    token_list *cur = tok_list;

	if ((cur->tok_value == K_CREATE) &&
			((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DROP) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_INSERT) &&
			((cur->next != NULL) && (cur->next->tok_value == K_INTO)) &&
			((cur->next->next->next != NULL) && (cur->next->next->next->tok_value == K_VALUES)))
	{
		printf("INSERT statement\n");
		cur_cmd = INSERT;
		cur = cur->next->next;
	}
	else if (cur->tok_value == K_SELECT)
	{
		printf("SELECT statement\n");
		cur_cmd = SELECT;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_UPDATE) &&
			((cur->next->next != NULL) && (cur->next->next->tok_value == K_SET)) &&
			((cur->next->next->next->next != NULL) && (cur->next->next->next->next->tok_value == S_EQUAL)))
	{
		printf("UPDATE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;	
	}
	else if ((cur->tok_value == K_DELETE) &&
			((cur->next != NULL) && (cur->next->tok_value == K_FROM)) &&
			((cur->next->next->next != NULL) && (cur->next->next->next->tok_value == K_WHERE)))
	{
		printf("DELETE statement\n");
		cur_cmd = DELETE;
		cur = cur->next->next;
	}
	else
    {
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd != INVALID_STATEMENT)
	{
		switch(cur_cmd)
		{
			case CREATE_TABLE:
				rc = sem_create_table(cur);
				break;
			case DROP_TABLE:
				rc = sem_drop_table(cur);
				break;
			case LIST_TABLE:
				rc = sem_list_tables();
				break;
			case LIST_SCHEMA:
				rc = sem_list_schema(cur);
				break;
			case INSERT:
				rc = sem_insert(cur);
				break;
			case SELECT:
				rc = sem_select(cur);
				break;
			case UPDATE:
				rc = sem_update(cur);
				break;
			case DELETE:
				rc = sem_delete(cur);
				break;
			default:
					; /* no action */
		}
	}
	
	return rc;
}

int sem_create_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];

	// Value to calculate total record size for table
	int record_size = 0;

	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for(i = 0; i < cur_id; i++)
						{
              /* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string)==0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
                /* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;
		
								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) &&
										  (cur->tok_value != K_NOT) &&
										  (cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
								  else
									{
										col_entry[cur_id].col_len = sizeof(int);
									
										// Add int to record size
										record_size += 5;

										if ((cur->tok_value == K_NOT) &&
											  (cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}	
										else if ((cur->tok_value == K_NOT) &&
											    (cur->next->tok_value == K_NULL))
										{					
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}
	
										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												  (cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
		                  					{
												if (cur->tok_value == S_RIGHT_PAREN)
												{
 													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of T_INT processing
								else
								{
									// It must be char() or varchar() 
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;
		
										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											
											// Add char to record size
											record_size += col_entry[cur_id].col_len + 1;

											cur = cur->next;
											
											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;
						
												if ((cur->tok_value != S_COMMA) &&
														(cur->tok_value != K_NOT) &&
														(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														  (cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
																(cur->next->tok_value == K_NULL))
													{					
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}
		
													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) &&															  (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
													    {
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));
	
				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + 
							sizeof(cd_entry) *	tab_entry.num_columns;
				    tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry,
							     (void*)&tab_entry,
									 sizeof(tpd_entry));
		
						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
									 (void*)col_entry,
									 sizeof(cd_entry) * tab_entry.num_columns);
	
						rc = add_tpd_to_list(new_entry);

						// File name
						char filename[MAX_IDENT_LEN+4] = "";
						strcat(filename, tab_entry.table_name);
						strcat(filename, ".tab");

						// Create file
						FILE *fp; 
						if ((fp = fopen(filename, "wbc")) == NULL) {
							rc = FILE_OPEN_ERROR;
							cur->tok_value = INVALID;
							printf("File could not be created: %s\n", filename);
						} else {
							// Populate table header information				
							table_file_header tab_header;
							tab_header.file_size = 24;
							tab_header.record_size = record_size  + (4 - (record_size % 4)); // Weird modulo to ensure record
							tab_header.num_records = 0;										 // size is stored in multiples of 4
							tab_header.record_offset = 25;
							tab_header.file_header_flag = 0;
							tab_header.tpd_ptr = NULL;

							fwrite(&tab_header, sizeof(table_file_header), 1, fp);
							fflush(fp);
							fclose(fp);
						}

						free(new_entry);
					}
				}
			}
		}
	}
  return rc;
}

int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);

				// Delete .tab file
				char filename[MAX_IDENT_LEN+4] = "";
				strcat(filename, cur->tok_string);
				strcat(filename, ".tab");
				
				// Delete file
				if (remove(filename) != 0) {
					printf("Error deleting file: %s\n", filename);
				}
			}
		}
	}

  return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

  return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
  {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			  (cur->tok_class != identifier) &&
				(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;
					
					if ((cur->tok_class != keyword) &&
						  (cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{ 
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
			            printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
  				            fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 
						}

						/* Next, write the cd_entry information */
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}
	
						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

  return rc;
} /* sem_list_schema */



// Insertion
// INSERT INTO table_name VALUES ({data_value})
int sem_insert(token_list *t_list) {
	int rc = 0;
	token_list *cur;
	bool column_done = false;
	char tablename[MAX_IDENT_LEN+4] = "";
	char filename[MAX_IDENT_LEN+4] = "";
	int cur_id = 0;

	cur = t_list;
	if ((cur->tok_class != identifier) &&
		(cur->tok_class != type_name))  
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
		return rc;
	}

	// ERROR: Table does not exist as well as retrieving table descriptor
	// Additionally, Retrieve tpd_entry from .bin file for column descriptors
	tpd_entry *tab_entry = NULL;
	if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL) {
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
		return rc;
	}

	printf("%d\n", cur->next->tok_value);
	if (cur->next == NULL || cur->next->tok_value != K_VALUES) {
		rc = INVALID_STATEMENT;
		cur->next->tok_value = INVALID;
		return rc;
	}

	/*
		== Allocate memory to read and write table data =================================
	*/

	// Get table name
	strcpy(tablename, cur->tok_string);

	// File name
	//char filename[MAX_IDENT_LEN+4] = "";
	strcat(filename, tablename);
	strcat(filename, ".tab");

	// Retrieve table data from file
	FILE *rp;
	if ((rp = fopen(filename, "rb")) == NULL) {
		rc = FILE_OPEN_ERROR;
		cur->tok_value = INVALID;
		return rc;
	}
	fseek(rp, 0, SEEK_END);
	size_t file_size = ftell(rp);
	fseek(rp, 0, SEEK_SET);

	// Allocate memory to store contents of file
	// - File header struct
	table_file_header *theader_ptr = (table_file_header*)malloc(sizeof(table_file_header));
	memset(theader_ptr, 0, sizeof(table_file_header));

	// Read header info into memory
	fread(&theader_ptr->file_size, sizeof(theader_ptr->file_size), 1, rp);
	fread(&theader_ptr->record_size, sizeof(theader_ptr->record_size), 1, rp);
	fread(&theader_ptr->num_records, sizeof(theader_ptr->num_records), 1, rp);
	fread(&theader_ptr->record_offset, sizeof(theader_ptr->record_offset), 1, rp);
	fread(&theader_ptr->file_header_flag, sizeof(theader_ptr->file_header_flag), 1, rp);
	fread(&theader_ptr->tpd_ptr, sizeof(theader_ptr->tpd_ptr), 1, rp);

	// - Table records (max 100 entries)
	void *table_content_buffer = (void*)calloc(MAX_TABLE_SIZE, theader_ptr->record_size);

	// If records exist then read them into memory as well
	//char temp_entry[theader_ptr->record_size];
	char *temp_entry = (char*)calloc(1, sizeof(theader_ptr->record_size));
	int i = 0;
	unsigned int num_records = 0;
	while (fread(&temp_entry, theader_ptr->record_size, 1, rp)) {
		memcpy((void*)((char*)table_content_buffer + (num_records * theader_ptr->record_size)), 
				(void*)&temp_entry, theader_ptr->record_size);

		// Reset temp_entry
		memset(&temp_entry, 0, sizeof(tpd_entry));
		num_records++;	
	}
	free(temp_entry);

	/*
	printf("Data before: ");
	for (int i = 0; i < file_size; ++i) {
		printf("%.02x", ((unsigned char*)table_content_buffer)[i]);
	}
	printf("\n");
	*/

	// Close file
	fclose(rp);

	/*
		== Retrieve column descriptors for error handling =================================
	*/

	// Skip keyword VALUES
	strcpy(tablename, cur->tok_string);
	cur = cur->next->next;

	// ERROR: No left paranthesis
	if (cur->tok_value != S_LEFT_PAREN) {
		rc = INVALID_INSERTION_DEFINITION;
		cur->tok_value = INVALID;

		free(theader_ptr);
		free(table_content_buffer);
		return rc;
	}
	cur = cur->next;


	/*
		== Iterate through columns and either error handle or store values in memory ================
	*/

	// Iterate through column descriptors and determine
	// whether descriptor aligns with new entry
	cd_entry *col_entry = NULL;
	bool entry_end = false;
	unsigned int record_offset;

	// Offset for Windows
	#ifdef _WIN32 || _WIN64
		record_offset = 4;
	#else
		record_offset = 0;
	#endif
	
	// Size of record value
	unsigned int size;

	// Variable for int conversion
	int conv_num;

	for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
		 i < tab_entry->num_columns; ++i, ++col_entry)
	{	

		// ERROR: entry ended but there are still columns that havent been filled
		if (entry_end) {
			rc = TOO_FEW_VALUES_IN_INSERT;
			cur->tok_value = INVALID;
			break;
		}

		// Determine type
		// Keyword := NULL
		if (cur->tok_class == keyword  && cur->tok_value == K_NULL) {

			// Check if field is allowed to be null
			if (col_entry->not_null == 1) {
				rc = NON_NULLABLE_FIELD;
				cur->tok_value = INVALID;
				break;
			}

			// Store in memory			
			size = 0;
			memcpy((void*)((char*)table_content_buffer + (theader_ptr->record_size * num_records) + record_offset), 
					(void*)&size, sizeof(unsigned int));

			// Set offset accordingly
			record_offset += col_entry->col_len + 1;

		}
		// Constant := STRING_LITERAL or INT_LITERAL
		else if (cur->tok_class == constant) {

			// Determine type
			switch (cur->tok_value) {
			case INT_LITERAL: // INT_LITERAL

				if (col_entry->col_type != T_INT) {
					rc = DATA_TYPE_MISMATCH;
					cur->tok_class = INVALID;
					free(theader_ptr);
					free(table_content_buffer);
					printf("ERROR: Data type mismatch\n");
					return rc;
				}

				//printf("int\n");

				// Store int in memory
				size = 4;

				// Convert string to int
				conv_num = atoi(cur->tok_string);

				// Size byte
				memcpy((void*)((char*)table_content_buffer + (theader_ptr->record_size * num_records) + record_offset),
						(void*)&size, sizeof(unsigned int));
				// Int data
				memcpy((void*)((char*)table_content_buffer + (theader_ptr->record_size * num_records) + record_offset + 1),
					(void*)&conv_num, sizeof(int));

				// Set offset
				record_offset += 5;

				break;
			case STRING_LITERAL: // STRING_LITERAL
				//printf("string\n");

				if (col_entry->col_type != T_CHAR) {
					rc = DATA_TYPE_MISMATCH;
					cur->tok_class = INVALID;
					free(theader_ptr);
					free(table_content_buffer);
					printf("ERROR: Data type mismatch\n");
					return rc;
				}

				// ERROR: string value entered exceeds column size capacity
				if (strlen(cur->tok_string) > col_entry->col_len) {
					rc = STRING_SIZE_EXCEEDED;
					cur->tok_value = INVALID;
					break;
				}

				// Store int in memory
				size = col_entry->col_len;

				// Size byte
				memcpy((void*)((char*)table_content_buffer + (num_records * theader_ptr->record_size) + record_offset),
					(void*)&size, sizeof(unsigned int));
				// string data
				memcpy((void*)((char*)table_content_buffer + (theader_ptr->record_size * num_records) + record_offset + 1),
				(void*)&cur->tok_string, col_entry->col_len);

				// Set offset
				record_offset += 1 + col_entry->col_len;

				break;
			default:
				rc = INVALID_COLUMN_DEFINITION;
				cur->tok_value = INVALID;
				break;
			}

			memset(col_entry, 0, sizeof(cd_entry*));
		}
		// ERROR: not a valid token 
		else {
			rc = INVALID_COLUMN_ENTRY;
			cur->tok_value = INVALID;			
		}

		//for (int i = 0; i < file_size; ++i) {
		//	printf("%.02x", ((unsigned char*)table_content_buffer)[i]);
		//}
		//printf("\n");

		// Invalid tokens break from loop
		if (cur->tok_value == INVALID) {
			break;		
		}

		// check for comma or right paren
		cur = cur->next;
		if (cur->tok_value == S_RIGHT_PAREN) {
			entry_end = true;
		} 
		// We want a comma
		else if (cur->tok_value == S_COMMA) {
			cur = cur->next;
		}
		// ERROR: missing comma
		else {
			rc = MISSING_COMMA_IN_INSERT;
			cur->tok_value = INVALID; 
			break;
		}
	}

	// Return if there was an error when looping through column descriptors
	if (cur->tok_value == INVALID) {
		free(theader_ptr);
		// Produces an error for some reason
		free(table_content_buffer);
		return rc;
	}


	/*
		=== Overwrite file with new memory =====================================================
	*/

	// Update number of records in header
	theader_ptr->num_records += 1;
	theader_ptr->file_size += theader_ptr->record_size;


	// I literaly do not know why i need to copy the data again but it doesnt work without it
	memset(filename, 0, MAX_IDENT_LEN+4);
	strcat(filename, tablename);
	strcat(filename, ".tab");

	FILE *wp;
	//printf("writing to file %s\n", filename);
	if ((wp = fopen(filename, "wbc")) == NULL) {
		cur->tok_value = INVALID;
		free(theader_ptr);
		free(table_content_buffer);
		return rc;
	}

	//fwrite(theader_ptr, sizeof(table_file_header), 1, wp);
	fwrite(theader_ptr, sizeof(table_file_header), 1, wp);
	fwrite(table_content_buffer, (theader_ptr->record_size) * theader_ptr->num_records, 1, wp);

	fflush(wp);
	fclose(wp);

	free(theader_ptr);
	free(table_content_buffer);

	printf("Insertion Successful.\n");

	return rc;
} /* sem_insert */





int sem_select(token_list *t_list) {
	int rc = 0;
	token_list *cur;
	char tablename[MAX_IDENT_LEN+4];
	cd_entry *col_entry = NULL;

	cur = t_list;
	
	// Booleans flags to handle input
	bool b_where = false;
	bool b_col_list = false;
	bool b_aggregate = false;
	bool b_order_by = false;
	bool b_order_desc = false;

	// SELECT * FROM table_name
	if (cur->tok_value == S_STAR && cur->next->next->next->tok_value == EOC) {
		cur = cur->next->next;
		rc = sem_select_all(cur);
		return rc;
	} 
	// SELECT * FROM table_name WHERE...
	else if (cur->tok_value == S_STAR && cur->next->next->next->tok_value == K_WHERE) {
		// token: FROM
		cur = cur->next; 
		b_where = true;
	} 
		// SELECT <aggregate> FROM table_name WHERE ..
	else if ((cur->tok_value == F_SUM || cur->tok_value == F_AVG || cur->tok_value == F_COUNT || cur->next->tok_value == S_LEFT_PAREN) &&
			 cur->next->next->next->next->next->next->tok_value == K_WHERE) 
	{	
		b_where = true;
		b_aggregate = true;
	}	
	// SELECT <aggregate> FROM table_name
	else if (cur->tok_value == F_SUM || cur->tok_value == F_AVG || cur->tok_value == F_COUNT || cur->next->tok_value == S_LEFT_PAREN) {
		b_aggregate = true;
	}
	// SELECT col1, col1, etc FROM table_name
	else if (cur->tok_value == IDENT) {
		b_col_list = true;
	}
	// Order by
	else {
		cur = cur->next;
	}
	

	/*
		== Parse aggregate ==================================================================
	*/
	aggregate_info agg_info;
	if (b_aggregate) {
		// Map aggregate type
		switch(cur->tok_value) {
			case F_SUM:
				agg_info.type = SUM;
			break;
			case F_AVG:
				agg_info.type = AVG;
			break;
			case F_COUNT:
				agg_info.type = COUNT;
			break;
			default:
				rc = INVALID_AGGREGATE;
				cur->tok_value = INVALID;
				return rc;
		}

		// Left param
		cur = cur->next;
		if (cur->tok_value != S_LEFT_PAREN) {
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}

		// Column value
		cur = cur->next;
		if (cur->tok_value == S_STAR) {
			if (agg_info.type == COUNT) {
				agg_info.count_star = true;
			}
			// ERROR: STAR can only be used on COUNT aggregate
			else {
				rc = STAR_ON_NONCOUNT_AGGREGATE;
				cur->tok_value = INVALID;
				return rc;
			}
		} 
		else {
			strcpy(agg_info.column_name, cur->tok_string);
		}

		// Right paran
		cur = cur->next;
		if (cur->tok_value != S_RIGHT_PAREN) {
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
			return rc;
		}	
		
		// token: from
		cur = cur->next;
	}


	/*
		=== Retrieve column names to be selected =============================================
	*/

	// Store columns to be displayed
	column_select_info columns[MAX_NUM_COL];
	int num_display_columns = 0;
	if (b_col_list) {
		while (true) {
			// store column name
			strcpy(columns[num_display_columns].col_name, cur->tok_string);
			++num_display_columns;

			// Either comma or FROM
			cur = cur->next;
			
			// ERROR: Not a comma
			if (cur->tok_value != S_COMMA && cur->tok_value != K_FROM) {
				rc = SYNTAX_ERROR;
				cur->tok_value = INVALID;
				return rc;
			} 
			// End of column list
			else if (cur->tok_value == K_FROM) {
				break;
			}

			// token: next column
			cur = cur->next;
		}
	}

	// token: table_name
	cur = cur->next;

	/*
		=== Retrieve data from table and table descriptor =================================
	*/

	// ERROR: Invalid table name
	if ((cur->tok_class != identifier) &&
		(cur->tok_class != type_name))  
	{
		printf("hi\n");
		printf("cur: %s\n", cur->tok_string);
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
		return rc;
	}

	// ERROR: Table does not exist
	tpd_entry *tab_entry = NULL;
	if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL) {
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
		return rc;
	}	

	// Get table name
	strcpy(tablename, cur->tok_string);

	// File name
	char filename[MAX_IDENT_LEN+4] = "";
	strcat(filename, tablename);
	strcat(filename, ".tab");

	// Retrieve table data from file
	FILE *rp;
	if ((rp = fopen(filename, "rb")) == NULL) {
		rc = FILE_OPEN_ERROR;
		cur->tok_value = INVALID;
		return rc;
	}
	fseek(rp, 0, SEEK_END);
	size_t file_size = ftell(rp);
	fseek(rp, 0, SEEK_SET);
	
	// Allocate memory to store contents of file
	// - File header struct
	table_file_header *theader_ptr = (table_file_header*)malloc(sizeof(table_file_header));
	memset(theader_ptr, 0, sizeof(table_file_header));

	// Read header info into memory
	fread(&theader_ptr->file_size, sizeof(theader_ptr->file_size), 1, rp);
	fread(&theader_ptr->record_size, sizeof(theader_ptr->record_size), 1, rp);
	fread(&theader_ptr->num_records, sizeof(theader_ptr->num_records), 1, rp);
	fread(&theader_ptr->record_offset, sizeof(theader_ptr->record_offset), 1, rp);
	fread(&theader_ptr->file_header_flag, sizeof(theader_ptr->file_header_flag), 1, rp);
	fread(&theader_ptr->tpd_ptr, sizeof(theader_ptr->tpd_ptr), 1, rp);

	/*
	printf("%d\n", theader_ptr->file_size);
	printf("%d\n", theader_ptr->record_size);
	printf("%d\n", theader_ptr->num_records);
	printf("%d\n", theader_ptr->record_offset);
	printf("%d\n", theader_ptr->file_header_flag);
	*/

	// - Table records (max 100 entries)
	void *table_content_buffer = (void*)calloc(MAX_TABLE_SIZE, theader_ptr->record_size);

	// If records exist then read them into memory as well
	unsigned char temp_entry[theader_ptr->record_size];
	int i = 0;
	unsigned int num_records = 0;
	while (fread(&temp_entry, theader_ptr->record_size, 1, rp)) {
		memcpy((void*)((char*)table_content_buffer - 4 + (num_records++ * theader_ptr->record_size)), 
				(void*)&temp_entry, theader_ptr->record_size);
		
		// Reset temp_entry
		memset(&temp_entry, 0, sizeof(tpd_entry));	
	}

	fclose(rp);


	/*
		== Handle WHERE clause to filter out data that will not be printed ===============================
	*/

	// Set where boolean
	if (cur->next->tok_value == K_WHERE) {
		b_where = true;
	}

	if (b_where) {
		bool where_final_boolean;
		bool b_and = false;
		bool b_or = false;
		bool second_statement_flag = false;
		// Structs for clause info
		where_clause_info clause1; clause1.used = true;
		where_clause_info clause2; clause2.used = false;
		where_clause_info *clause_ptr = &clause1;
		do {
			// Handle cases for AND and OR
			if (cur->tok_value == K_AND) {
				b_and = true;
				second_statement_flag = true;
				cur = cur->next;
				clause_ptr = &clause2;
				clause_ptr->used = true;
			} else if (cur->tok_value == K_OR) {
				b_or = true;
				second_statement_flag = true;
				cur = cur->next;
				clause_ptr = &clause2;
				clause_ptr->used = true;
			} else {
				// Column name
				cur = cur->next->next;
			}



			// Iterate through each column to find the matching column index
			clause_ptr->col_index = 0;
			clause_ptr->col_offset = 0;
			bool col_found = false;
			bool wrong_type = true;
			bool not_nullable;
			for (col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
				clause_ptr->col_index < tab_entry->num_columns; ++col_entry) 
			{
				// If column was found then break
				if (strcasecmp(col_entry->col_name, cur->tok_string) == 0) {
					col_found = true;

					// Check to make sure data is of the right type
					if ((col_entry->col_type == T_INT && cur->next->next->tok_value == INT_LITERAL) ||
						(col_entry->col_type == T_CHAR && cur->next->next->tok_value == STRING_LITERAL) ||
						(col_entry->col_type == T_CHAR && cur->next->next->tok_value == IDENT) ||
						(cur->next->next->tok_value == K_NULL || 
						(cur->next->next->tok_value == K_NOT && cur->next->next->next->tok_value == K_NULL))) {
						wrong_type = false;
						clause_ptr->col_type = cur->next->next->tok_value;
						clause_ptr->col_length = col_entry->col_len;
						not_nullable = col_entry->not_null;
					}	

					break;
				}	
				
				// Increment column index and increase column offset
				++clause_ptr->col_index;
				clause_ptr->col_offset += col_entry->col_len + 1;
			}

			// ERROR: column does not exist in the current table
			if (!col_found) {
				rc = INVALID_COLUMN_DEFINITION;
				cur->tok_value = INVALID;
				free(theader_ptr);
				free(table_content_buffer);
				return rc;
			}
			// ERROR: column exists but entered value is of wrong type
			else if (wrong_type) {
				rc = INVALID_COLUMN_TYPE;
				cur->tok_value = INVALID;
				free(theader_ptr);
				free(table_content_buffer);
				return rc;
			}

			// ERROR: checking null for non nullable type
			if (not_nullable && (cur->next->next->tok_value == K_NULL || cur->next->next->tok_value == K_NOT)) {
				rc = NON_NULLABLE_FIELD;
				cur->next->next->tok_value = INVALID;
				free(theader_ptr);
				free(table_content_buffer);
				return rc;
			}

			// ERROR: not a relational operator
			if ((cur->next->tok_class != symbol && 
				(cur->next->tok_value != S_EQUAL &&
				cur->next->tok_value != S_LESS &&
				cur->next->tok_value != S_GREATER)) &&
				(cur->next->tok_value != K_IS)) {
				rc = NO_RELATIONAL_OPERATOR;
				cur->next->tok_value = INVALID;
				free(theader_ptr);
				free(table_content_buffer);
				return rc;
			} 
			
			// Set operation to be performed
			clause_ptr->relational_type[0] = cur->next->tok_value;
			
			// token: value or NOT
			cur = cur->next->next;

			// Check for is_not clause
			if (clause_ptr->relational_type[0] == K_IS && 
				cur->tok_value == K_NOT && 
				cur->next->tok_value == K_NULL)
			{
				clause_ptr->relational_type[1] = K_NOT;
				clause_ptr->col_type = K_NULL;
				// token: value
				cur = cur->next;
			}	

			// Copy the value 
			strcpy(clause_ptr->value, cur->tok_string);
			
			// AND or OR or EOC or ORDER
			cur = cur->next;
		} while ((cur->tok_value == K_AND || cur->tok_value == K_OR) && !second_statement_flag);


		// Now iterate through data and delete data that does not match
		int where_eval_num = 0;
		for (int row_index = 0; row_index < theader_ptr->num_records; ++row_index) {

			clause_ptr = &clause1;
			//bool no_clauses = false;
			int ci = 0;
			while (clause_ptr->used && ci < 2) {

				if (clause_ptr->col_type == INT_LITERAL) {
					// Provided data
					int where_num = atoi(clause_ptr->value);

					// Skip null values
					unsigned int size;
					memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + clause_ptr->col_offset), sizeof(unsigned int));
					if (size == 0) {
						clause_ptr->row_eval = false;
						// Switch to clause2
						clause_ptr = &clause2;
						ci++;
						continue;
					}

					// Data from table
					int tab_num;
					memcpy(&tab_num, (int*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + clause_ptr->col_offset + 1), sizeof(int));

					// Determine operator to use and perform operation
					switch (clause_ptr->relational_type[0]) {
						case S_EQUAL:
							//printf("%d = %d\n", where_num, tab_num);
							if (tab_num != where_num) {
								clause_ptr->row_eval = false;
							} else {
								clause_ptr->row_eval = true;
							}
						break;
						case S_LESS:
							//printf("%d < %d\n", where_num, tab_num);
							if (!(tab_num < where_num)) {
								clause_ptr->row_eval = false;
							} else {
								clause_ptr->row_eval = true;
							}
						break;
						case S_GREATER:
							//printf("%d > %d\n", where_num, tab_num);
							if (!(tab_num > where_num)) {
								clause_ptr->row_eval = false;
							} else {
								clause_ptr->row_eval = true;
							}
						break;
					}
				}
				else if (clause_ptr->col_type == STRING_LITERAL) {
					// Skip null values
					unsigned int size;
					memcpy(&size, (int*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + clause_ptr->col_offset), sizeof(unsigned int));
					if (size == 0) {
						clause_ptr->row_eval = false;
						// Switch to clause2
						clause_ptr = &clause2;
						ci++;
						continue;
					}

					char *tab_string = (char*)calloc(1, clause_ptr->col_length);
					memcpy(tab_string, (char*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + clause_ptr->col_offset + 1), clause_ptr->col_length);

					// Perform operation to determine relational solution
					switch(clause_ptr->relational_type[0]) {
						case S_EQUAL:
							if (strcmp(tab_string, clause_ptr->value) == 0) {
								clause_ptr->row_eval = true;
							} else {
								clause_ptr->row_eval = false;
							}
						break;
						case S_LESS:
							if (strcmp(tab_string, clause_ptr->value) < 0) {
								clause_ptr->row_eval = true;
							} else {
								clause_ptr->row_eval = false;
							}
						break;
						case S_GREATER:
							if (strcmp(tab_string, clause_ptr->value) > 0) {
								clause_ptr->row_eval = true;
							} else {
								clause_ptr->row_eval = false;
							}
						break;					
					}

					free(tab_string);
				}
				else if (clause_ptr->col_type == K_NULL) {

					unsigned int size;
					memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + clause_ptr->col_offset), sizeof(unsigned int));

					//printf("size: %d\n", size);

					if (clause_ptr->relational_type[0] == S_EQUAL) {
						// ERROR: NOT cannot follow =
						if (clause_ptr->relational_type[1] == K_NOT) {
							rc = NOT_AFTER_EQUALS;
							cur->next->next->next->tok_value = INVALID;
							free(theader_ptr);
							free(table_content_buffer);
							return rc;
						}
						
						// is null
						if (size == 0) {
							clause_ptr->row_eval = true;
						} else {
							clause_ptr->row_eval = false;
						}

					} 
					else if (clause_ptr->relational_type[0] == K_IS) {
						if (clause_ptr->relational_type[1] == K_NOT) {
							if (size > 0) {
								clause_ptr->row_eval = true;
							} else {
								clause_ptr->row_eval = false;
							}
						} else {
							if (size == 0) {
								clause_ptr->row_eval = true;
							} else {
								clause_ptr->row_eval = false;
							}							
						}
					}
				}
				else {
					printf("Program should not get here\n");
				}

				// Switch to clause2
				clause_ptr = &clause2;
				ci++;
			}

			// Determine whether row will be deleted from memory or not
			bool b_final_eval;
			if (clause2.used) {
				if (b_and) {
					b_final_eval = clause1.row_eval && clause2.row_eval;
				} else if (b_or) {
					b_final_eval = clause1.row_eval || clause2.row_eval;
				}
			} else {
				b_final_eval = clause1.row_eval;
			}

			//printf("equation 1: %d\n", b_final_eval);

			// Delete row from memory
			if (!b_final_eval) {
				// Delete memory
				memset((void*)((char*)table_content_buffer + (theader_ptr->record_size * row_index)), 0, theader_ptr->record_size);

				// If row was not at the end, then we need to move the rest of the memory to fill the gap
				if (row_index <= theader_ptr->num_records-1 ) {
					memcpy((void*)((char*)table_content_buffer + (theader_ptr->record_size * row_index)),
						(void*)((char*)table_content_buffer + (theader_ptr->record_size * (row_index+1))),
						theader_ptr->record_size * (theader_ptr->num_records - row_index));
				}

				// Update record number and file size
				theader_ptr->num_records--;
				theader_ptr->file_size -= theader_ptr->record_size;
				row_index--;
			}
		}
	}

	// Set order by boolean
	if (cur->next != NULL) {
		if (cur->next->tok_value == K_ORDER && cur->next->next->tok_value == K_BY) {
			b_order_by = true;
			if (cur->next->next->next->next != NULL) {
				if (cur->next->next->next->next->tok_value == K_DESC) {
					b_order_desc = true;
				}
			}
		}
	}


	// array for row order and set to -1
	int order_by_indices[100];
	memset(order_by_indices, -1, sizeof(int) * 100);
	int null_indices[100];
	memset(null_indices, -1, sizeof(int) * 100);


	// Handle order by by finding the column location
	if (b_order_by) {
		// Find offset of column to by checked
		int order_by_offset = 0;
		int order_by_length = 0;
		bool match = false;
		int type = -1;
		for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
			i < tab_entry->num_columns; ++i, ++col_entry)
		{
			// check if column matched
			if (strcasecmp(col_entry->col_name, cur->next->next->next->tok_string) == 0) {
				match = true;
				type = col_entry->col_type;
				order_by_length = col_entry->col_len;
				break;
			}
			order_by_offset += col_entry->col_len + 1;
		}

		// ERROR: Order by colun does not exist
		if (!match) {
			rc = ORDER_BY_COLUMN_DOES_NOT_EXIST;
			cur->next->next->next->tok_value = INVALID;
			free(theader_ptr);
			free(table_content_buffer);
			return rc;
		}

		// Populate array with original row order
		int null_num = 0;
		for (int row_index = 0; row_index < theader_ptr->num_records; ++row_index) {
			// Check if NULL
			unsigned int size;
			memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + order_by_offset), sizeof(unsigned int));

			// If NULL then store in null_indices 
			if (size == 0) {
				for (int j = 0; j < 100; ++j) {
					if (null_indices[j] == -1) {
						null_indices[j] = row_index;
						null_num++;
						break;
					}
				}
			}
			// Normal row store in order_by_indices
			else {
				order_by_indices[row_index] = row_index;	
			}
		}
		
		/*int k = 0;
		while (order_by_indices[k] != -1) {
			printf("%d ", order_by_indices[k]);
			k++;
		}
		printf("\n");*/


		// Find ordering for order by
		bool swapped = false;
		for (int r = 0; r < (theader_ptr->num_records - null_num) - 1; ++r) {
			swapped = false;
			for (int j = 0; j < (theader_ptr->num_records - null_num) - r - 1; ++j) {
				// Integer
				if (type == T_INT) {
					int int_buffer1;
					memcpy(&int_buffer1, (int*)((char*)table_content_buffer + (theader_ptr->record_size * order_by_indices[j]) + order_by_offset + 1), sizeof(int));

					int int_buffer2;
					memcpy(&int_buffer2, (int*)((char*)table_content_buffer + (theader_ptr->record_size * order_by_indices[j+1]) + order_by_offset + 1), sizeof(int));

					// swap
					if (b_order_desc) {
						if (int_buffer1 < int_buffer2) {
							int temp;
							temp = order_by_indices[j];
							order_by_indices[j] = order_by_indices[j+1];
							order_by_indices[j+1] = temp;
							swapped = true;
						}
					}
					else {
						if (int_buffer1 > int_buffer2) {
							int temp;
							temp = order_by_indices[j];
							order_by_indices[j] = order_by_indices[j+1];
							order_by_indices[j+1] = temp;
							swapped = true;
						}
					}

				}
				// String literal
				else {
					char* str_buffer1 = (char*)calloc(1, order_by_length);
					memcpy(str_buffer1, (char*)((char*)table_content_buffer + (theader_ptr->record_size * order_by_indices[j]) + order_by_offset + 1), order_by_length);

					char* str_buffer2 = (char*)calloc(1, order_by_length);
					memcpy(str_buffer2, (char*)((char*)table_content_buffer + (theader_ptr->record_size * order_by_indices[j+1]) + order_by_offset + 1), order_by_length);

					// swap
					if (b_order_desc) {
						if (strcmp(str_buffer1, str_buffer2) < 0) {
							int temp;
							temp = order_by_indices[j];
							order_by_indices[j] = order_by_indices[j+1];
							order_by_indices[j+1] = temp;
						}
					}
					else {
						if (strcmp(str_buffer1, str_buffer2) > 0) {
							int temp;
							temp = order_by_indices[j];
							order_by_indices[j] = order_by_indices[j+1];
							order_by_indices[j+1] = temp;
						}						
					}

					free(str_buffer1);
					free(str_buffer2);
				}
			}

			if (!swapped)
				break;
		}
		
	}
	
	// Iterate through column and determine proper column index
	if (b_col_list) {
		for (i = 0; i < num_display_columns; ++i) {

			// Iterate through each column
			int j = 0;
			bool match = false;
			int offset = 0;
			for (j = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
				j < tab_entry->num_columns; ++j, ++col_entry)
			{	
				// If column matched then record column
				if (strcasecmp(columns[i].col_name, col_entry->col_name) == 0) {
					columns[i].col_index = j;
					columns[i].col_offset = offset;
					match = true;
					break;
				}

				offset += col_entry->col_len + 1;
			}

			// ERROR: Column did not match
			if (!match) {
				rc = COLUMN_NOT_EXIST;
				cur->tok_value = INVALID;
				free(theader_ptr);
				free(table_content_buffer);
				return rc;
			}
		}	
	}
	
	// Iterate through each column and determine the column offset of aggregate and whether it is valid
	agg_info.col_offset = 0;
	if (b_aggregate && !agg_info.count_star) {
		bool match = false;
		bool right_type = false;
		col_entry = NULL;
		for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
			i < tab_entry->num_columns; ++i, ++col_entry)
		{	
			if (strcasecmp(agg_info.column_name, col_entry->col_name) == 0) {
				match = true;

				// If column name and type match then the match is okay for AVG and SUM
				if (col_entry->col_type == T_INT && ((agg_info.type == AVG || agg_info.type == SUM))) {
					right_type = true;

				}
				
				// Determine if column is nullable to potentially speed up calculations
				agg_info.col_non_nullable = col_entry->not_null;

				break;
			}

			agg_info.col_offset += col_entry->col_len + 1;
		}

		if (!match) {
			rc = AGGREGATE_COLUMN_DOES_NOT_EXIST;
			cur->tok_value = INVALID;
			free(theader_ptr);
			free(table_content_buffer);
			return rc;
		}

		if (!right_type && agg_info.type != COUNT) {
			rc = AGGREGATE_OF_NON_INTEGER_TYPE;
			cur->tok_value = INVALID;
			free(theader_ptr);
			free(table_content_buffer);
			return rc;
		}
	}

	/*
		=== Print Column Names ==========================================
	*/

	if (b_aggregate) {
		printf("\n\n");
		switch (agg_info.type) {
			case SUM: printf("SUM("); break;
			case AVG: printf("AVG("); break;
			case COUNT: printf("COUNT("); break;
		}

		if (agg_info.count_star) {
			printf("*)\n");
		} else {
			printf("%s)\n", agg_info.column_name);
		}

	} else {
		col_entry = NULL;
		printf("\n\n");
		for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
			i < tab_entry->num_columns; ++i, ++col_entry)
		{
			// Print specific columns
			if (b_col_list) {
				bool skip = true;
				// Iterate through columns and determine whether column should be printed
				for (int j = 0; j < num_display_columns; ++j) {
					if (columns[j].col_index == i) {
						skip = false;
					}
				}
				if (skip) {
					continue;
				}
			}

			printf("%s ", col_entry->col_name);
			
			int col_name_len = strlen(col_entry->col_name);
			int col_width = col_entry->col_len;

			int j = col_name_len;
			while(j < col_width) {
				printf(" ");
				++j;
			}
			
		}
		printf("\n");
	}



	/*
		=== Print Column Spacers ========================================
	*/

	if (b_aggregate) {
		printf("-----------------\n");
	} else {
		col_entry = NULL;
		for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
			i < tab_entry->num_columns; ++i, ++col_entry)
		{	
			// Print specific columns
			if (b_col_list) {
				bool skip = true;
				// Iterate through columns and determine whether column should be printed
				for (int j = 0; j < num_display_columns; ++j) {
					if (columns[j].col_index == i) {
						skip = false;
					}
				}
				if (skip) {
					continue;
				}
			}

			int col_width = col_entry->col_len;
			int col_name_len = strlen(col_entry->col_name);

			if (col_width < col_name_len)
				col_width = col_name_len;

			int j = 0;
			while(j < col_width) {
				printf("-");
				++j;
			}
			
			printf(" ");
		}
		printf("\n");
	}

	/*
		=== Print record data ===========================================
	*/

	if (b_aggregate) {
		if (agg_info.count_star) { // COUNT(*)
			printf("%d\n", theader_ptr->num_records);
		}
		else if (agg_info.type == COUNT) {
			if (agg_info.col_non_nullable == 1) { // if non nullable then it is just the sum
				printf("%d\n", theader_ptr->num_records);
			} 
			else {
				int count = 0;
				for (i = 0; i<theader_ptr->num_records; ++i) {
					unsigned int size;
					memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * i) + agg_info.col_offset), sizeof(unsigned int));

					if (size != 0) {
						++count;	
					}
				}
				printf("%d\n", count);
			}

		}
		else if (agg_info.type == AVG) {
			int total = 0;
			int num_recs = 0;
			for (i = 0; i<theader_ptr->num_records; ++i) {
				unsigned int size;
				memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * i) + agg_info.col_offset), sizeof(unsigned int));

				if (size != 0) {
					int number;
					memcpy(&number, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * i) + agg_info.col_offset + 1), sizeof(int));

					total += number;
					++num_recs;	
				}
			}
			printf("%d\n", total / num_recs);
		}
		else if (agg_info.type == SUM) {
			int sum = 0;
			for (i = 0; i<theader_ptr->num_records; ++i) {
				unsigned int size;
				memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * i) + agg_info.col_offset), sizeof(unsigned int));

				if (size != 0) {
					int number;
					memcpy(&number, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * i) + agg_info.col_offset + 1), sizeof(int));

					sum += number;
				}
			}
			printf("%d\n", sum);
		}
	}
	else {
		
		// Order by printing
		if (b_order_by) {
			// Iterate through records and print
			// For each record
			char size;
			char str_buffer[MAX_IDENT_LEN + 4];
			memset(str_buffer, 0, MAX_IDENT_LEN + 4);
			int int_buffer;

			// Unix specific offset
			int record_offset = 0;
			i = 0;

			// for descending nulls go first
			if (b_order_desc) {
				// Null rows
				i = 0;
				while (null_indices[i] != -1) {
					//int record_offset = 0;
					
					// For each column
					col_entry = NULL;
					int j;
					
					for (j = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
						j < tab_entry->num_columns; ++j, ++col_entry)
					{
						// Print specific columns
						int col_print_index;
						if (b_col_list) {
							bool skip = true;
							// Iterate through columns and determine whether column should be printed
							for (col_print_index = 0; col_print_index < num_display_columns; ++col_print_index) {
								if (columns[col_print_index].col_index == j) {
									skip = false;
									break;
								}
							}
							if (skip) {
								continue;
							}
						}

						if (col_entry->col_type == T_INT) { // integer (right aligned)
							if (b_col_list) { // select col
								memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * null_indices[i]) + columns[col_print_index].col_offset), sizeof(unsigned int));
							} else { // select *
								memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * null_indices[i]) + record_offset), sizeof(unsigned int));
							}

							// NULL Value
							if ((int)size == 0) {
								int l = 0;
								while (l++ < strlen(col_entry->col_name) - 4) {
									printf(" ");
								}
								printf("NULL ");
							
								record_offset += 5;
							} 
							// Normal int
							else {
								if (b_col_list) { // select col
									memcpy(&int_buffer, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * null_indices[i]) + columns[col_print_index].col_offset + 1), sizeof(int));
								} else { // select *
									memcpy(&int_buffer, (int*)((char*)table_content_buffer + (theader_ptr->record_size * null_indices[i]) + record_offset + 1), sizeof(int));
								}

								// Count number of digits in integer
								int n = int_buffer;
								int num_digits = 0;
								while (n != 0) {
									n /= 10;
									++num_digits;
								}

								// Add padding
								int l = 0;
								while (l++ < col_entry->col_len - num_digits || l <= strlen(col_entry->col_name) - num_digits) {
									printf(" ");
								}

								printf("%d ", int_buffer);

								record_offset += (int)size + 1;
							}
						}
						else if (col_entry->col_type == T_CHAR) { // fixed length string (left aligned)

							if (b_col_list) { // select col
								memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * null_indices[i]) + columns[col_print_index].col_offset), sizeof(unsigned int));
							} else { // select *
								memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * null_indices[i]) + record_offset), sizeof(unsigned int));
							}

							// NULL Value
							if ((int)size == 0) {
								printf("NULL ");
								int l = 0;
								while (l++ < strlen(col_entry->col_name) - 4) {
									printf(" ");
								}

								record_offset += 1 + col_entry->col_len;
							} 
							// Normal Value
							else {
								if (b_col_list) { // select col
									memcpy(&str_buffer, (char*)((char*)table_content_buffer + (theader_ptr->record_size * null_indices[i]) + columns[col_print_index].col_offset + 1), col_entry->col_len);
								} else { // select *
									memcpy(&str_buffer, (char*)((char*)table_content_buffer + (theader_ptr->record_size * null_indices[i]) + record_offset + 1), col_entry->col_len);
								}
								printf("%s ", str_buffer);


								// Add spacing
								int l = strlen(str_buffer);
								while (l < col_entry->col_len || l < strlen(col_entry->col_name)) {
									printf(" ");
									++l;
								}

								record_offset += (int)size + 1;

								memset(str_buffer, 0, MAX_IDENT_LEN + 4);
							}
							
						}
						else { // Invalid data type
							
						}

					}

					record_offset = 0;
					printf("\n");
				
					i++;
				}			
			}


			// Non null rows
			i = 0;
			while (order_by_indices[i] != -1) {
				//int record_offset = 0;
				
				// For each column
				col_entry = NULL;
				int j;
				
				for (j = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
					j < tab_entry->num_columns; ++j, ++col_entry)
				{
					// Print specific columns
					int col_print_index;
					if (b_col_list) {
						bool skip = true;
						// Iterate through columns and determine whether column should be printed
						for (col_print_index = 0; col_print_index < num_display_columns; ++col_print_index) {
							if (columns[col_print_index].col_index == j) {
								skip = false;
								break;
							}
						}
						if (skip) {
							continue;
						}
					}

					if (col_entry->col_type == T_INT) { // integer (right aligned)
						if (b_col_list) { // select col
							memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * order_by_indices[i]) + columns[col_print_index].col_offset), sizeof(unsigned int));
						} else { // select *
							memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * order_by_indices[i]) + record_offset), sizeof(unsigned int));
						}

						// NULL Value
						if ((int)size == 0) {
							int l = 0;
							while (l++ < strlen(col_entry->col_name) - 4) {
								printf(" ");
							}
							printf("NULL ");
						
							record_offset += 5;
						} 
						// Normal int
						else {
							if (b_col_list) { // select col
								memcpy(&int_buffer, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * order_by_indices[i]) + columns[col_print_index].col_offset + 1), sizeof(int));
							} else { // select *
								memcpy(&int_buffer, (int*)((char*)table_content_buffer + (theader_ptr->record_size * order_by_indices[i]) + record_offset + 1), sizeof(int));
							}

							// Count number of digits in integer
							int n = int_buffer;
							int num_digits = 0;
							while (n != 0) {
								n /= 10;
								++num_digits;
							}

							// Add padding
							int l = 0;
							while (l++ < col_entry->col_len - num_digits || l <= strlen(col_entry->col_name) - num_digits) {
								printf(" ");
							}

							printf("%d ", int_buffer);

							record_offset += (int)size + 1;
						}
					}
					else if (col_entry->col_type == T_CHAR) { // fixed length string (left aligned)

						if (b_col_list) { // select col
							memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * order_by_indices[i]) + columns[col_print_index].col_offset), sizeof(unsigned int));
						} else { // select *
							memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * order_by_indices[i]) + record_offset), sizeof(unsigned int));
						}

						// NULL Value
						if ((int)size == 0) {
							printf("NULL ");
							int l = 0;
							while (l++ < strlen(col_entry->col_name) - 4) {
								printf(" ");
							}

							record_offset += 1 + col_entry->col_len;
						} 
						// Normal Value
						else {
							if (b_col_list) { // select col
								memcpy(&str_buffer, (char*)((char*)table_content_buffer + (theader_ptr->record_size * order_by_indices[i]) + columns[col_print_index].col_offset + 1), col_entry->col_len);
							} else { // select *
								memcpy(&str_buffer, (char*)((char*)table_content_buffer + (theader_ptr->record_size * order_by_indices[i]) + record_offset + 1), col_entry->col_len);
							}
							printf("%s ", str_buffer);


							// Add spacing
							int l = strlen(str_buffer);
							while (l < col_entry->col_len || l < strlen(col_entry->col_name)) {
								printf(" ");
								++l;
							}

							record_offset += (int)size + 1;

							memset(str_buffer, 0, MAX_IDENT_LEN + 4);
						}
						
					}
					else { // Invalid data type
						
					}
				}

				record_offset = 0;
				printf("\n");
				
				i++;
			}

			if (!b_order_desc) {
				// Null rows
				i = 0;
				while (null_indices[i] != -1) {
					//int record_offset = 0;
					
					// For each column
					col_entry = NULL;
					int j;
					
					for (j = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
						j < tab_entry->num_columns; ++j, ++col_entry)
					{
						// Print specific columns
						int col_print_index;
						if (b_col_list) {
							bool skip = true;
							// Iterate through columns and determine whether column should be printed
							for (col_print_index = 0; col_print_index < num_display_columns; ++col_print_index) {
								if (columns[col_print_index].col_index == j) {
									skip = false;
									break;
								}
							}
							if (skip) {
								continue;
							}
						}

						if (col_entry->col_type == T_INT) { // integer (right aligned)
							if (b_col_list) { // select col
								memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * null_indices[i]) + columns[col_print_index].col_offset), sizeof(unsigned int));
							} else { // select *
								memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * null_indices[i]) + record_offset), sizeof(unsigned int));
							}

							// NULL Value
							if ((int)size == 0) {
								int l = 0;
								while (l++ < strlen(col_entry->col_name) - 4) {
									printf(" ");
								}
								printf("NULL ");
							
								record_offset += 5;
							} 
							// Normal int
							else {
								if (b_col_list) { // select col
									memcpy(&int_buffer, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * null_indices[i]) + columns[col_print_index].col_offset + 1), sizeof(int));
								} else { // select *
									memcpy(&int_buffer, (int*)((char*)table_content_buffer + (theader_ptr->record_size * null_indices[i]) + record_offset + 1), sizeof(int));
								}

								// Count number of digits in integer
								int n = int_buffer;
								int num_digits = 0;
								while (n != 0) {
									n /= 10;
									++num_digits;
								}

								// Add padding
								int l = 0;
								while (l++ < col_entry->col_len - num_digits || l <= strlen(col_entry->col_name) - num_digits) {
									printf(" ");
								}

								printf("%d ", int_buffer);

								record_offset += (int)size + 1;
							}
						}
						else if (col_entry->col_type == T_CHAR) { // fixed length string (left aligned)

							if (b_col_list) { // select col
								memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * null_indices[i]) + columns[col_print_index].col_offset), sizeof(unsigned int));
							} else { // select *
								memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * null_indices[i]) + record_offset), sizeof(unsigned int));
							}

							// NULL Value
							if ((int)size == 0) {
								printf("NULL ");
								int l = 0;
								while (l++ < strlen(col_entry->col_name) - 4) {
									printf(" ");
								}

								record_offset += 1 + col_entry->col_len;
							} 
							// Normal Value
							else {
								if (b_col_list) { // select col
									memcpy(&str_buffer, (char*)((char*)table_content_buffer + (theader_ptr->record_size * null_indices[i]) + columns[col_print_index].col_offset + 1), col_entry->col_len);
								} else { // select *
									memcpy(&str_buffer, (char*)((char*)table_content_buffer + (theader_ptr->record_size * null_indices[i]) + record_offset + 1), col_entry->col_len);
								}
								printf("%s ", str_buffer);


								// Add spacing
								int l = strlen(str_buffer);
								while (l < col_entry->col_len || l < strlen(col_entry->col_name)) {
									printf(" ");
									++l;
								}

								record_offset += (int)size + 1;

								memset(str_buffer, 0, MAX_IDENT_LEN + 4);
							}
							
						}
						else { // Invalid data type
							
						}

					}

					record_offset = 0;
					printf("\n");
				
					i++;
				}
			}
			

			// Print number of records
			printf("%d record(s) found.\n\n", theader_ptr->num_records);
		} else {
			// Iterate through records and print
			// For each record
			char size;
			char str_buffer[MAX_IDENT_LEN + 4];
			memset(str_buffer, 0, MAX_IDENT_LEN + 4);
			int int_buffer;

			// Unix specific offset
			int record_offset = 0;
			
			for (i = 0; i < theader_ptr->num_records; ++i) {
				//int record_offset = 0;
				
				// For each column
				col_entry = NULL;
				int j;

				for (j = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
					j < tab_entry->num_columns; ++j, ++col_entry)
				{
					// Print specific columns
					int col_print_index;
					if (b_col_list) {
						bool skip = true;
						// Iterate through columns and determine whether column should be printed
						for (col_print_index = 0; col_print_index < num_display_columns; ++col_print_index) {
							if (columns[col_print_index].col_index == j) {
								skip = false;
								break;
							}
						}
						if (skip) {
							continue;
						}
					}

					if (col_entry->col_type == T_INT) { // integer (right aligned)
						if (b_col_list) { // select col
							memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * i) + columns[col_print_index].col_offset), sizeof(unsigned int));
						} else { // select *
							memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * i) + record_offset), sizeof(unsigned int));
						}

						// NULL Value
						if ((int)size == 0) {
							int l = 0;
							while (l++ < strlen(col_entry->col_name) - 4) {
								printf(" ");
							}
							printf("NULL ");
						
							record_offset += 5;
						} 
						// Normal int
						else {
							if (b_col_list) { // select col
								memcpy(&int_buffer, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * i) + columns[col_print_index].col_offset + 1), sizeof(int));
							} else { // select *
								memcpy(&int_buffer, (int*)((char*)table_content_buffer + (theader_ptr->record_size * i) + record_offset + 1), sizeof(int));
							}

							// Count number of digits in integer
							int n = int_buffer;
							int num_digits = 0;
							while (n != 0) {
								n /= 10;
								++num_digits;
							}

							// Add padding
							int l = 0;
							while (l++ < col_entry->col_len - num_digits || l <= strlen(col_entry->col_name) - num_digits) {
								printf(" ");
							}

							printf("%d ", int_buffer);

							record_offset += (int)size + 1;
						}
					}
					else if (col_entry->col_type == T_CHAR) { // fixed length string (left aligned)

						if (b_col_list) { // select col
							memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * i) + columns[col_print_index].col_offset), sizeof(unsigned int));
						} else { // select *
							memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * i) + record_offset), sizeof(unsigned int));
						}

						// NULL Value
						if ((int)size == 0) {
							printf("NULL ");
							int l = 0;
							while (l++ < strlen(col_entry->col_name) - 4) {
								printf(" ");
							}

							record_offset += 1 + col_entry->col_len;
						} 
						// Normal Value
						else {
							if (b_col_list) { // select col
								memcpy(&str_buffer, (char*)((char*)table_content_buffer + (theader_ptr->record_size * i) + columns[col_print_index].col_offset + 1), col_entry->col_len);
							} else { // select *
								memcpy(&str_buffer, (char*)((char*)table_content_buffer + (theader_ptr->record_size * i) + record_offset + 1), col_entry->col_len);
							}
							printf("%s ", str_buffer);


							// Add spacing
							int l = strlen(str_buffer);
							while (l < col_entry->col_len || l < strlen(col_entry->col_name)) {
								printf(" ");
								++l;
							}

							record_offset += (int)size + 1;

							memset(str_buffer, 0, MAX_IDENT_LEN + 4);
						}
						
					}
					else { // Invalid data type
						
					}

				}

				record_offset = 0;
				printf("\n");
			}

			// Print number of records
			printf("%d record(s) found.\n\n", theader_ptr->num_records);
		}

		
	}


	//printf("tablename: %s\n", tablename);

	free(theader_ptr);
	free(table_content_buffer);

	return rc;
} /* sem_select */


int sem_select_all(token_list *t_list) {
	int rc = 0;
	token_list *cur;
	char tablename[MAX_IDENT_LEN+4];

	cur = t_list;

	/*
		=== Retrieve data from table and column descriptors =================================
	*/

	// ERROR: Invalid table name
	if ((cur->tok_class != identifier) &&
		(cur->tok_class != type_name))  
	{
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
		return rc;
	}

	// ERROR: Table does not exist
	tpd_entry *tab_entry = NULL;
	if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL) {
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
		return rc;
	}	

	// Get table name
	strcpy(tablename, cur->tok_string);

	// File name
	char filename[MAX_IDENT_LEN+4] = "";
	strcat(filename, tablename);
	strcat(filename, ".tab");

	// Retrieve table data from file
	FILE *rp;
	if ((rp = fopen(filename, "rb")) == NULL) {
		rc = FILE_OPEN_ERROR;
		cur->tok_value = INVALID;
		return rc;
	}
	fseek(rp, 0, SEEK_END);
	size_t file_size = ftell(rp);
	fseek(rp, 0, SEEK_SET);
	
	// Allocate memory to store contents of file
	// - File header struct
	table_file_header *theader_ptr = (table_file_header*)malloc(sizeof(table_file_header));
	memset(theader_ptr, 0, sizeof(table_file_header));

	// Read header info into memory
	fread(&theader_ptr->file_size, sizeof(theader_ptr->file_size), 1, rp);
	fread(&theader_ptr->record_size, sizeof(theader_ptr->record_size), 1, rp);
	fread(&theader_ptr->num_records, sizeof(theader_ptr->num_records), 1, rp);
	fread(&theader_ptr->record_offset, sizeof(theader_ptr->record_offset), 1, rp);
	fread(&theader_ptr->file_header_flag, sizeof(theader_ptr->file_header_flag), 1, rp);
	fread(&theader_ptr->tpd_ptr, sizeof(theader_ptr->tpd_ptr), 1, rp);

	/*
	printf("%d\n", theader_ptr->file_size);
	printf("%d\n", theader_ptr->record_size);
	printf("%d\n", theader_ptr->num_records);
	printf("%d\n", theader_ptr->record_offset);
	printf("%d\n", theader_ptr->file_header_flag);
	*/

	// - Table records (max 100 entries)
	void *table_content_buffer = (void*)calloc(MAX_TABLE_SIZE, theader_ptr->record_size);

	// If records exist then read them into memory as well
	unsigned char temp_entry[theader_ptr->record_size];
	int i = 0;
	unsigned int num_records = 0;
	while (fread(&temp_entry, theader_ptr->record_size, 1, rp)) {
		memcpy((void*)((char*)table_content_buffer + (num_records++ * theader_ptr->record_size)), 
				(void*)&temp_entry, theader_ptr->record_size);
		
		// Reset temp_entry
		memset(&temp_entry, 0, sizeof(tpd_entry));	
	}

	fclose(rp);

	/*
		=== Print Column Names ==========================================
	*/

	// Iterate through columns and print column names
	cd_entry *col_entry = NULL;
	printf("\n\n");
	for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
		 i < tab_entry->num_columns; ++i, ++col_entry)
	{
		printf("%s ", col_entry->col_name);
		
		int col_name_len = strlen(col_entry->col_name);
		int col_width = col_entry->col_len;

		int j = col_name_len;
		while(j < col_width) {
			printf(" ");
			++j;
		}
		
	}
	printf("\n");

	/*
		=== Print Column Spacers ========================================
	*/

	col_entry = NULL;
	for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
		 i < tab_entry->num_columns; ++i, ++col_entry)
	{		
		int col_width = col_entry->col_len;
		int col_name_len = strlen(col_entry->col_name);

		if (col_width < col_name_len)
			col_width = col_name_len;

		int j = 0;
		while(j < col_width) {
			printf("-");
			++j;
		}
		
		printf(" ");
	}
	printf("\n");

	/*
		=== Print record data ===========================================
	*/

/*
	for (int i = 0; i < file_size; ++i) {
		printf("%.02x", ((unsigned char*)table_content_buffer)[i]);
	}
	printf("\n");
*/

	// Iterate through records and print
	// For each record
	char size;
	char str_buffer[MAX_IDENT_LEN + 4];
	memset(str_buffer, 0, MAX_IDENT_LEN + 4);
	int int_buffer;

	// Unix specific offset
	int record_offset;
	record_offset = 4;
	
	for (i = 0; i < theader_ptr->num_records; ++i) {
		//int record_offset = 0;
		
		// For each column
		col_entry = NULL;
		int j;

		for (j = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
			j < tab_entry->num_columns; ++j, ++col_entry)
		{
			if (col_entry->col_type == T_INT) { // integer (right aligned)

				memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * i) + record_offset), sizeof(unsigned int));

				// NULL Value
				if ((int)size == 0) {
					int l = 0;
					while (l++ < strlen(col_entry->col_name) - 4) {
						printf(" ");
					}
					printf("NULL ");

					record_offset += 5;
				} 
				// Normal int
				else {
					memcpy(&int_buffer, (int*)((char*)table_content_buffer + (theader_ptr->record_size * i) + record_offset + 1), sizeof(int));
					
					// Count number of digits in integer
					int n = int_buffer;
					int num_digits = 0;
					while (n != 0) {
						n /= 10;
						++num_digits;
					}

					// Add padding
					int l = 0;
					while (l++ < col_entry->col_len - num_digits || l <= strlen(col_entry->col_name) - num_digits) {
						printf(" ");
					}

					printf("%d ", int_buffer);

					record_offset += (int)size + 1;
				}
			}
			else if (col_entry->col_type == T_CHAR) { // fixed length string (left aligned)
				
				memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * i) + record_offset), sizeof(unsigned int));
				
				// NULL Value
				if ((int)size == 0) {
					printf("NULL ");
					int l = 0;
					while (l++ < strlen(col_entry->col_name) - 4) {
						printf(" ");
					}

					record_offset += 1 + col_entry->col_len;
				} 
				// Normal Value
				else {
					memcpy(&str_buffer, (char*)((char*)table_content_buffer + (theader_ptr->record_size * i) + record_offset + 1), col_entry->col_len);
					printf("%s ", str_buffer);


					// Add spacing
					int l = strlen(str_buffer);
					while (l < col_entry->col_len || l < strlen(col_entry->col_name)) {
						printf(" ");
						++l;
					}

					record_offset += (int)size + 1;

					memset(str_buffer, 0, MAX_IDENT_LEN + 4);
				}
				
			}
			else { // Invalid data type
				
			}

		}

		record_offset = 4;
		printf("\n");
	}

	// Print number of records
	printf("%d record(s) found.\n\n", theader_ptr->num_records);

	// Free memory
	free(theader_ptr);
	free(table_content_buffer);

	return rc;
} /* sem_select_all */




int sem_update(token_list *t_list) {
	bool b_where = false;
	int where_operator = -1;
	int where_type = -1;
	unsigned int where_index = 0;
	unsigned int where_offset = 0;
	int where_not_nullable;
	char *where_value = NULL;	

	int rc = 0;
	token_list *cur = t_list;

	// ERROR: Invalid table name
	if ((cur->tok_class != identifier) &&
		(cur->tok_class != type_name))  
	{
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
		return rc;
	}

	// ERROR: Table does not exist
	tpd_entry *tab_entry = NULL;
	if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL) {
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
		return rc;
	}

	// Store table name
	char tablename[MAX_IDENT_LEN+4] = "";
	strcpy(tablename, cur->tok_string);

	// skip to column name token
	cur = cur->next->next;

	// Check if column exists within table as well
	// as well as determining the offset that is required for changing the value later
	cd_entry *col_entry = NULL;
	bool column_exists = false;
	bool wrong_type = true;

	// Offset and index needed later
	unsigned int column_offset = 0;
	unsigned int col_index = 0;
	unsigned int value_len;
	int not_nullable = 0;
	for (col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
		col_index < tab_entry->num_columns; ++col_entry) 
	{
		// Column exists
		if (strcmp(col_entry->col_name, cur->tok_string) == 0) {
			column_exists = true;

			// Check to make sure data is of the right type
			if ((col_entry->col_type == T_INT && cur->next->next->tok_value == INT_LITERAL) ||
				(col_entry->col_type == T_CHAR && cur->next->next->tok_value == STRING_LITERAL) ||
				(col_entry->col_type == T_CHAR && cur->next->next->tok_value == IDENT) ||
				cur->next->next->tok_value == K_NULL) {
				wrong_type = false;
				value_len = col_entry->col_len;
				not_nullable = col_entry->not_null;
			}

			break;
		}

		// update the column offset for column before the column being updated
		++col_index;
		column_offset += col_entry->col_len + 1;
	}


	// ERROR: column does not exist in the current table
	if (!column_exists) {
		rc = INVALID_COLUMN_DEFINITION;
		cur->tok_value = INVALID;
		return rc;
	}
	// ERROR: column exists but entered value is of wrong type
	else if (wrong_type) {
		rc = INVALID_COLUMN_TYPE;
		cur->tok_value = INVALID;
		return rc;
	}

	// ERROR: trying to make non nullable field null
	if (not_nullable == 1 && cur->next->next->tok_value == K_NULL) {
		rc = NON_NULLABLE_FIELD;
		cur->next->next->tok_value = INVALID;
		return rc;
	}

	// Store name of column
	char column_name[MAX_IDENT_LEN+4] = "";
	strcpy(column_name, cur->tok_string);

	// cur is now the value
	// Store the values type and string data
	cur = cur->next->next;
	int value_type = cur->tok_value;

	/*
		== Detect valid WHERE clause ==============================================
	*/

	// Check for WHERE clause as this will determine where the program will branch
	if ((cur->next != NULL) && (cur->next->tok_value == K_WHERE)) {
		
		// ERROR: no table name
		if (cur->next->next == NULL) {
			rc = INVALID_UPDATE_STATEMENT;
			cur->next->next->tok_value = INVALID;
			return rc;
		}

		// Search for column name
		// Find if data value type aligns with column data type
		wrong_type = true;
		column_exists = false;
		for (col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
		where_index < tab_entry->num_columns; ++col_entry) 
		{
			// Column exists
			if (strcmp(col_entry->col_name, cur->next->next->tok_string) == 0) {
				column_exists = true;

				// Check to make sure data is of the right type
				if ((col_entry->col_type == T_INT && cur->next->next->next->next->tok_value == INT_LITERAL) ||
					(col_entry->col_type == T_CHAR && cur->next->next->next->next->tok_value == STRING_LITERAL) ||
					(col_entry->col_type == T_CHAR && cur->next->next->next->next->tok_value == IDENT) ||
					(cur->next->next->next->next->tok_value == K_NULL || 
					(cur->next->next->next->next->tok_value == K_NOT && cur->next->next->next->next->next->tok_value == K_NULL))) {
					wrong_type = false;
					where_type = cur->next->next->next->next->tok_value;
					where_not_nullable = col_entry->not_null;
				}

				break;
			}

			// Update index and offset for column to be compared
			++col_index;
			++where_index;
			where_offset += col_entry->col_len + 1;
		}

		// ERROR: column does not exist in the current table
		if (!column_exists) {
			rc = INVALID_COLUMN_DEFINITION;
			cur->tok_value = INVALID;
			return rc;
		}
		// ERROR: column exists but entered value is of wrong type
		else if (wrong_type) {
			rc = INVALID_COLUMN_TYPE;
			cur->tok_value = INVALID;
			return rc;
		}

		// ERROR: not a relational operator
		if ((cur->next->next->next->tok_class != symbol && 
			(cur->next->next->next->tok_value != S_EQUAL &&
			cur->next->next->next->tok_value != S_LESS &&
			cur->next->next->next->tok_value != S_GREATER)) &&
			(cur->next->next->next->tok_value != K_IS)) {
			rc = NO_RELATIONAL_OPERATOR;
			cur->next->next->next->tok_value = INVALID;
			return rc;
		} 
		else {
			// Set operation to be performed
			where_operator = cur->next->next->next->tok_value;	
		}

		// Valid where clause
		b_where = true;
	} 

	/*
		== Retrieve table data ========================================
	*/
	
	// filename
	//char filename[MAX_IDENT_LEN+4] = "";
	char* filename = (char*)calloc(1, MAX_IDENT_LEN+4);
	strcat(filename, tablename);
	strcat(filename, ".tab");
	// Retrieve table data from file
	FILE *rp;
	if ((rp = fopen(filename, "rb")) == NULL) {
		rc = FILE_OPEN_ERROR;
		cur->tok_value = INVALID;
		return rc;
	}
	fseek(rp, 0, SEEK_END);
	size_t file_size = ftell(rp);
	fseek(rp, 0, SEEK_SET);

	// Header information
	table_file_header *theader_ptr = (table_file_header*)calloc(1, sizeof(table_file_header) + 4);

	fread(&theader_ptr->file_size, sizeof(theader_ptr->file_size), 1, rp);
	fread(&theader_ptr->record_size, sizeof(theader_ptr->record_size), 1, rp);
	fread(&theader_ptr->num_records, sizeof(theader_ptr->num_records), 1, rp);
	fread(&theader_ptr->record_offset, sizeof(theader_ptr->record_offset), 1, rp);
	fread(&theader_ptr->file_header_flag, sizeof(theader_ptr->file_header_flag), 1, rp);
	fread(&theader_ptr->tpd_ptr, sizeof(theader_ptr->tpd_ptr), 1, rp);

	// ERROR: There are zero rows to check
	if (theader_ptr->num_records == 0) {
		//rc = NO_ROWS_FOUND;
		//cur->tok_value = INVALID;
		printf("Table is empty. No records were updated.\n");
		free(theader_ptr);
		return rc;
	}

	// Table record buffer
	void *table_content_buffer = (void*)calloc(MAX_TABLE_SIZE, theader_ptr->record_size);

	// If records exist then read them into memory as well
	char *temp_entry = (char*)calloc(1, sizeof(theader_ptr->record_size));
	int i = 0;
	unsigned int num_records = 0;
	while (fread(&temp_entry, theader_ptr->record_size, 1, rp)) {
		memcpy((void*)((char*)table_content_buffer - 4 + (num_records * theader_ptr->record_size)), 
				(void*)&temp_entry, theader_ptr->record_size);

		// Reset temp_entry
		memset(&temp_entry, 0, sizeof(tpd_entry));
		num_records++;	
	}
	free(temp_entry);

	fclose(rp);


	/*
		== Change data in rows ====================================
	*/

	unsigned int row_index = 0;
	int row_offset = 0;
	int max_size = theader_ptr->file_size - 24;
	int where_eval_num = 0;
	for (;;) {
		// Break if end of data block has been reached
		if ((theader_ptr->record_size * row_index) + column_offset + 1 >= max_size) {
			break;
		}

		// Check validity for statements with where clause
		bool b_where_pass = false;
		if (b_where) {
			if (where_type == INT_LITERAL) {
				
				// Skip null values
				unsigned int size;
				memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + where_offset), sizeof(unsigned int));
				if (size == 0) {
					++row_index;
					continue;
				}

				// Provided data
				int where_num = atoi(cur->next->next->next->next->tok_string);

				// Data from table
				int tab_num;
				memcpy(&tab_num, (int*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + where_offset + 1), sizeof(int));

				// Determine operator to use and perform operation
				switch (where_operator) {
					case S_EQUAL:
						//printf("%d = %d\n", where_num, tab_num);
						if (tab_num != where_num) {
							++row_index;
							continue;
						} else {
							++where_eval_num;
						}
					break;
					case S_LESS:
						//printf("%d < %d\n", where_num, tab_num);
						if (!(tab_num < where_num)) {
							++row_index;
							continue;
						} else {
							++where_eval_num;
						}
					break;
					case S_GREATER:
						//printf("%d > %d\n", where_num, tab_num);
						if (!(tab_num > where_num)) {
							++row_index;
							continue;
						} else {
							++where_eval_num;
						}
					break;
				}
			}
			else if (where_type == STRING_LITERAL || where_type == IDENT) {

				// Skip null values
				unsigned int size;
				memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + where_offset), sizeof(unsigned int));
				if (size == 0) {
					++row_index;
					continue;
				}

				int tab_str_len = strlen(cur->next->next->next->next->tok_string);
				char *tab_string = (char*)calloc(1, tab_str_len);
				memcpy(tab_string, (char*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + where_offset + 1), tab_str_len);

				// Perform operation to determine relational solution
				switch(where_operator) {
					case S_EQUAL:
						if (strcmp(tab_string, cur->next->next->next->next->tok_string) == 0) {
							++where_eval_num;
						} else {
							free(tab_string);
							++row_index;
							continue;
						}
					break;
					case S_LESS:
						if (strcmp(tab_string, cur->next->next->next->next->tok_string) < 0) {
							++where_eval_num;
						} else {
							free(tab_string);
							++row_index;
							continue;
						}
					break;
					case S_GREATER:
						if (strcmp(tab_string, cur->next->next->next->next->tok_string) > 0) {
							++where_eval_num;
						} else {
							free(tab_string);
							++row_index;
							continue;
						}
					break;					
				}

				free(tab_string);
			}
			else if (where_type == K_NULL || K_NOT) {
				unsigned int size;
				memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + where_offset), sizeof(unsigned int));

				if (where_not_nullable == 1) {
					printf("Checking for null in non nullable field.\n");
					printf("No record(s) updated.\n");
					return rc;
				}

				if (where_operator == S_EQUAL) {

					// ERROR: NOT cannot follow =
					if (where_type == K_NOT) {
						rc = NOT_AFTER_EQUALS;
						cur->next->next->next->next->next->tok_value = INVALID;
						free(theader_ptr);
						free(table_content_buffer);
						return rc;
					}
					
					// is null
					if (size == 0) {
						++where_eval_num;
					} else {
						++row_index;
						continue;
					}

				} else if (where_operator == K_IS) {
					if (where_type == K_NULL) {
						if (size == 0) {
							++where_eval_num;
						} else {
							++row_index;
							continue;
						}
					} else if (where_type == K_NOT) {
						if (size > 0) {
							++where_eval_num;
						} else {
							++row_index;
							continue;
						}
					}
				}
			}
			else {
				printf("Program should not get here\n");
				printf("type: %d\n", where_type);
				printf("operator: %d\n", where_operator);
			}
		}

		// Handle writing for each type
		if (value_type == INT_LITERAL) {

			// Convert data
			int conv_data = atoi(cur->tok_string);

			// Store data in memory
			memcpy((void*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + column_offset + 1), 
					(void*)&conv_data, sizeof(int));
				
		}
		else if (value_type == STRING_LITERAL || value_type == IDENT) {


			unsigned int length = strlen(cur->tok_string);

			// Store size and data in memory
			memcpy((void*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + column_offset), 
					(void*)&length, sizeof(unsigned int));				
			memcpy((void*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + column_offset + 1), 
					(void*)&cur->tok_string, value_len);

		}
		else if (value_type == K_NULL) {

			unsigned int null_len = 0;

			// Store size and data in memory
			memcpy((void*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + column_offset), 
					(void*)&null_len, sizeof(unsigned int));
			memset((void*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + column_offset + 1),
					0, value_len);

		}
		else {
			// ERROR: unsupported type
			rc = INVALID_TYPE_NAME;
			cur->tok_value = INVALID;
			free(theader_ptr);
			free(table_content_buffer);
			return rc;
		}
		
		++row_index;
	}

	// No records were updated
	if (b_where && where_eval_num == 0) {
		printf("No record(s) updated.\n");
		free(theader_ptr);
		free(table_content_buffer);
		return rc;
	}

	/*
		== Write manipulated data to file =============================
	*/

	FILE *wp;
	
	//memset(filename, 0, MAX_IDENT_LEN+4);
	//strcat(filename, tablename);
	//strcat(filename, ".tab");

	//if ((wp = fopen("test.tab", "wbc")) == NULL) {
	if ((wp = fopen(filename, "wbc")) == NULL) {
		rc = FILE_OPEN_ERROR;
		cur->tok_value = INVALID;
		free(theader_ptr);
		free(table_content_buffer);
		return rc;
	}

	fwrite(theader_ptr, sizeof(table_file_header) + 4, 1, wp);
	fwrite(table_content_buffer, theader_ptr->record_size * theader_ptr->num_records, 1, wp);

	fflush(wp);
	fclose(wp);

	printf("Update Successful.\n");
	if (b_where) {
		printf("%d record(s) updated.\n", where_eval_num);
	} else {
		printf("%d record(s) updated.\n", theader_ptr->num_records);
	}

	free(filename);
	free(theader_ptr);
	free(table_content_buffer);		

	return rc;
} /* sem_update */



int sem_delete(token_list *t_list) {
	bool b_where = false;
	int where_operator = -1;
	int where_type = -1;
	unsigned int where_index = 0;
	unsigned int where_offset = 0;
	int where_not_nullable;
	char *where_value = NULL;	

	int rc = 0;
	token_list *cur = t_list;

	// ERROR: Invalid table name
	if ((cur->tok_class != identifier) &&
		(cur->tok_class != type_name))  
	{
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
		return rc;
	}

	// ERROR: Table does not exist
	tpd_entry *tab_entry = NULL;
	if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL) {
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
		return rc;
	}

	// Store table name
	char tablename[MAX_IDENT_LEN+4] = "";
	strcpy(tablename, cur->tok_string);

	/*
		== Parse WHERE clause ================================
	*/

	// Column name
	cur = cur->next->next;

	// Find if column exists in table
	cd_entry *col_entry = NULL;
	bool column_exists = false;
	bool wrong_type = true;

	// Offset and index needed later
	unsigned int column_offset = 0;
	unsigned int col_index = 0;
	unsigned int value_len;
	int not_nullable = 0;
	for (col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
		col_index < tab_entry->num_columns; ++col_entry) 
	{
		// Column exists
		if (strcmp(col_entry->col_name, cur->tok_string) == 0) {
			column_exists = true;

			// Check to make sure data is of the right type
			if ((col_entry->col_type == T_INT && cur->next->next->tok_value == INT_LITERAL) ||
				(col_entry->col_type == T_CHAR && cur->next->next->tok_value == STRING_LITERAL) ||
				(col_entry->col_type == T_CHAR && cur->next->next->tok_value == IDENT) ||
				(cur->next->next->tok_value == K_NULL || 
				(cur->next->next->tok_value == K_NOT && cur->next->next->next->tok_value == K_NULL))) {
				wrong_type = false;
				value_len = col_entry->col_len;
				where_type = cur->next->next->tok_value;
				not_nullable = col_entry->not_null;
			}

			break;
		}

		// update the column offset for column before the column being updated
		++col_index;
		column_offset += col_entry->col_len + 1;
	}


	// ERROR: column does not exist in the current table
	if (!column_exists) {
		rc = INVALID_COLUMN_DEFINITION;
		cur->tok_value = INVALID;
		return rc;
	}
	// ERROR: column exists but entered value is of wrong type
	else if (wrong_type) {
		rc = INVALID_COLUMN_TYPE;
		cur->tok_value = INVALID;
		return rc;
	}

	// ERROR: trying to make non nullable field null
	if (not_nullable == 1 && cur->next->next->tok_value == K_NULL) {
		rc = NON_NULLABLE_FIELD;
		cur->next->next->tok_value = INVALID;
		return rc;
	}


	// ERROR: not a relational operator
	if ((cur->next->tok_class != symbol && 
		(cur->next->tok_value != S_EQUAL &&
		cur->next->tok_value != S_LESS &&
		cur->next->tok_value != S_GREATER)) &&
		(cur->next->tok_value != K_IS)) {
		rc = NO_RELATIONAL_OPERATOR;
		cur->next->tok_value = INVALID;
		return rc;
	} 
	else {
		// Set operation to be performed
		where_operator = cur->next->tok_value;	
	}

	/*
		== Retrieve table data ====================================================
	*/

	// filename
	//char filename[MAX_IDENT_LEN+4] = "";
	char* filename = (char*)calloc(1, MAX_IDENT_LEN+4);
	strcat(filename, tablename);	 
	strcat(filename, ".tab");

	// Retrieve table data from file
	FILE *rp;
	if ((rp = fopen(filename, "rb")) == NULL) {
		rc = FILE_OPEN_ERROR;
		cur->tok_value = INVALID;
		return rc;
	}
	fseek(rp, 0, SEEK_END);
	size_t file_size = ftell(rp);
	fseek(rp, 0, SEEK_SET);

	// Header information
	table_file_header *theader_ptr = (table_file_header*)calloc(1, sizeof(table_file_header) + 4);

	fread(&theader_ptr->file_size, sizeof(theader_ptr->file_size), 1, rp);
	fread(&theader_ptr->record_size, sizeof(theader_ptr->record_size), 1, rp);
	fread(&theader_ptr->num_records, sizeof(theader_ptr->num_records), 1, rp);
	fread(&theader_ptr->record_offset, sizeof(theader_ptr->record_offset), 1, rp);
	fread(&theader_ptr->file_header_flag, sizeof(theader_ptr->file_header_flag), 1, rp);
	fread(&theader_ptr->tpd_ptr, sizeof(theader_ptr->tpd_ptr), 1, rp);

	// ERROR: There are zero rows to check
	if (theader_ptr->num_records == 0) {
		//rc = NO_ROWS_FOUND;
		//cur->tok_value = INVALID;
		printf("Table is empty. No records were updated.\n");
		free(theader_ptr);
		return rc;
	}

	// Table record buffer
	void *table_content_buffer = (void*)calloc(MAX_TABLE_SIZE, theader_ptr->record_size);

	// If records exist then read them into memory as well
	char *temp_entry = (char*)calloc(1, sizeof(theader_ptr->record_size));
	int i = 0;
	unsigned int num_records = 0;
	while (fread(&temp_entry, theader_ptr->record_size, 1, rp)) {
		memcpy((void*)((char*)table_content_buffer - 4 + (num_records * theader_ptr->record_size)), 
				(void*)&temp_entry, theader_ptr->record_size);

		// Reset temp_entry
		memset(&temp_entry, 0, sizeof(tpd_entry));
		num_records++;	
	}
	free(temp_entry);

	fclose(rp);

	
	/*
		== Find valid rows and delete them ========================================
	*/

	unsigned int row_index = 0;
	int row_offset = 0;
	int max_size = theader_ptr->file_size - 24;
	int where_eval_num = 0;
	for (;;) {
		// Break if end of data block has been reached
		if ((theader_ptr->record_size * row_index) + column_offset + 1 >= max_size) {
			break;
		}

		// Check validity for statements with where clause
		bool b_where_pass = false;
		if (where_type == INT_LITERAL) {
			// Provided data
			int where_num = atoi(cur->next->next->tok_string);

			// Skip null values
			unsigned int size;
			memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + column_offset), sizeof(unsigned int));
			if (size == 0) {
				++row_index;
				continue;
			}

			// Data from table
			int tab_num;
			memcpy(&tab_num, (int*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + column_offset + 1), sizeof(int));

			// Determine operator to use and perform operation
			switch (where_operator) {
				case S_EQUAL:
					//printf("%d = %d\n", where_num, tab_num);
					if (tab_num != where_num) {
						++row_index;
						continue;
					} else {
						++where_eval_num;
					}
				break;
				case S_LESS:
					//printf("%d < %d\n", where_num, tab_num);
					if (!(tab_num < where_num)) {
						++row_index;
						continue;
					} else {
						++where_eval_num;
					}
				break;
				case S_GREATER:
					//printf("%d > %d\n", where_num, tab_num);
					if (!(tab_num > where_num)) {
						++row_index;
						continue;
					} else {
						++where_eval_num;
					}
				break;
			}
		}
		else if (where_type == STRING_LITERAL || where_type == IDENT) {

			// Skip null values
			unsigned int size;
			memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + column_offset), sizeof(unsigned int));
			if (size == 0) {
				++row_index;
				continue;
			}

			int tab_str_len = strlen(cur->next->next->tok_string);
			char *tab_string = (char*)calloc(1, tab_str_len);
			memcpy(tab_string, (char*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + column_offset + 1), tab_str_len);


			// Perform operation to determine relational solution
			switch(where_operator) {
				case S_EQUAL:
					if (strcmp(tab_string, cur->next->next->tok_string) == 0) {
						++where_eval_num;
					} else {
						free(tab_string);
						++row_index;
						continue;
					}
				break;
				case S_LESS:
					if (strcmp(tab_string, cur->next->next->tok_string) < 0) {
						++where_eval_num;
					} else {
						free(tab_string);
						++row_index;
						continue;
					}
				break;
				case S_GREATER:
					if (strcmp(tab_string, cur->next->next->tok_string) > 0) {
						++where_eval_num;
					} else {
						free(tab_string);
						++row_index;
						continue;
					}
				break;					
			}

			free(tab_string);
		}
		else if (where_type == K_NULL || K_NOT) {

			unsigned int size;
			memcpy(&size, (unsigned int*)((char*)table_content_buffer + (theader_ptr->record_size * row_index) + column_offset), sizeof(unsigned int));

			if (where_not_nullable == 1) {
				printf("Checking for null in non nullable field.\n");
				printf("No record(s) deleted.\n");
				return rc;
			}

			if (where_operator == S_EQUAL) {

				// ERROR: NOT cannot follow =
				if (where_type == K_NOT) {
					rc = NOT_AFTER_EQUALS;
					cur->next->next->next->tok_value = INVALID;
					free(theader_ptr);
					free(table_content_buffer);
					return rc;
				}
				
				// is null
				if (size == 0) {
					++where_eval_num;
				} else {
					++row_index;
					continue;
				}

			} else if (where_operator == K_IS) {
				if (where_type == K_NULL) {
					if (size == 0) {
						++where_eval_num;
					} else {
						++row_index;
						continue;
					}
				} else if (where_type == K_NOT) {
					if (size > 0) {
						++where_eval_num;
					} else {
						++row_index;
						continue;
					}
				}
			}
		}
		else {
			printf("Program should not get here\n");
			printf("type: %d\n", where_type);
			printf("operator: %d\n", where_operator);
		}

		/*
			== Delete row ====================================================
		*/

		// Delete memory
		memset((void*)((char*)table_content_buffer + (theader_ptr->record_size * row_index)), 0, theader_ptr->record_size);

		// If row was not at the end, then we need to move the rest of the memory to fill the gap
		if (row_index <= theader_ptr->num_records-1 ) {
			memcpy((void*)((char*)table_content_buffer + (theader_ptr->record_size * row_index)),
				   (void*)((char*)table_content_buffer + (theader_ptr->record_size * (row_index+1))),
			   	   theader_ptr->record_size * (theader_ptr->num_records - row_index));
		}

		// Update record number and file size
		theader_ptr->num_records--;
		theader_ptr->file_size -= theader_ptr->record_size;
		
		if (row_index == theader_ptr->num_records) {
			break;
		}
		// Dont update row because the row index will be the same except for last
		//++row_index;	
	}

	/*
		== Write manipulated data to file ====================================
	*/

	FILE *wp;
	//if ((wp = fopen("test.tab", "wbc")) == NULL) {
	printf("filename: %s\n", filename);
	if ((wp = fopen(filename, "wbc")) == NULL) {
		rc = FILE_OPEN_ERROR;
		cur->tok_value = INVALID;
		free(theader_ptr);
		free(table_content_buffer);
		return rc;
	}

	fwrite(theader_ptr, sizeof(table_file_header) + 4, 1, wp);
	fwrite(table_content_buffer, theader_ptr->record_size * theader_ptr->num_records, 1, wp);

	fflush(wp);
	fclose(wp);

	if (where_eval_num == 0) {
		printf("No records deleted.\n");
	} else {
		printf("Delete Successful.\n");
		printf("%d record(s) deleted.\n", where_eval_num);
	}

	free(filename);
	free(theader_ptr);
	free(table_content_buffer);

	return rc;
} /* sem_delete */



int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
//	struct _stat file_stat;
	struct stat file_stat;

  /* Open for read */
  if((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
    else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));
			
			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
		// fstat(_fileno(fhandle), &file_stat);
		fstat(fileno(fhandle), &file_stat);
		printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}

		}
	}
    
	return rc;
}
	
int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
  else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
			  else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								     1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								     old_size - cur->tpd_size -
										 (sizeof(tpd_list) - sizeof(tpd_entry)),
								     1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
					 	g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
									 1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						   has already subtracted the cur->tpd_size, therefore it will
						   point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							   next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
										 old_size - cur->tpd_size -
										 ((char*)cur - (char*)g_tpd_list),							     
								     1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}

				
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}
	
	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}

from lark import Lark, Transformer, exceptions, Tree
from berkeleydb import db
from Database import *
from CustomException import *
import re
from datetime import datetime

# Input Prompt
PROMPT = "DB_2020-16634> "

# Data Types
INT = "int"
DATE = "date"
CHAR = "char"
NULL = "null"

# LOGIC OPERATORS
AND = "and"
OR = "or"

# COMPARISON OPERATORS
GREATER_THAN = ">"
LESS_THAN = "<"
EQUAL = "="
NOT_EQUAL = "!="
GREATER_OR_EQUAL = ">="
LESS_OR_EQUAL = "<="

# Declaring Transformer class and transform methods
class MyTransformer(Transformer):
    def __init__(self, database):
        super().__init__()
        self.db = database

    # Helper Functions Handling table_name
    def table_name_exists(self, table_name):
        """
        Checks if a given table name exists in the database by verifying the presence of its key.

        Parameters:
        - table_name (str): The name of the table to check for existence.

        Returns:
        - bool: Returns True if the table exists in the database, 
                False otherwise.
        """
        table_key = SCHEMA_KEY_PREFIX + table_name
        if self.db.key_exists(table_key):
            return True
        else:
            return False

    def get_table_schema(self, table_name):
        """
        Retrieves the schema string for a specified table from the database.

        Parameters:
        - table_name (str): The name of the table whose schema is to be retrieved.

        Returns:
        - str or None: The schema string of the specified table if it exists, or None if the table does not exist or no schema is found. #FIX: integrate with table_name_exists() and table exists error
        """
        table_key = SCHEMA_KEY_PREFIX + table_name
        schema_str = self.db.get_table_schema(table_key)
        return schema_str  

    def get_table_column_names(self, table_name):
        schema = self.get_table_schema(table_name)
        
        # Collect column names from schema 
        column_names = []
        for column_def in schema.split("|")[0].split(';'):
            column_name = column_def.split(":")[0]
            column_names.append(column_name)
        
        return column_names

    # Helper Functions Handling column_name
    def column_exists_in_schema(self, column_name, schema):
        """
        Checks if a given column name exists within a specified schema list.
        Parameters:
        - column_name (str): The name of the column to check for.
        - schema (list of dict): A list of dictionaries, each representing a column schema
        where each dictionary contains at least a 'name' key.

        Returns:
        - bool: Returns True if the column name is found in any dictionary within the schema list;
                Returns False otherwise.

        Usage:
        - CREATE: before full schema string is created.
        """
        if any(col['name'] == column_name for col in schema):
            return True
        else:
            return False

    def column_exists_in_table_name(self, column_name, table_name):
        """
        Checks if a given column name exists within a schema named as table_name.
        Parameters:
        - column_name (str): The name of the column to check for.
        - table_name (str): The name of table representing a schema

        Returns:
        - bool: Returns True if the column name is found in any dictionary within the schema list;
                Returns False otherwise.

        Usage:
        - CREATE: before full schema string is created.
        """
        schema_str = self.get_table_schema(table_name)
        
        column_definitions = schema_str.split("|")[0].split(";")
        column_names = [col_def.split(":")[0] for col_def in column_definitions]
        if column_name in column_names:
            return True
        else:
            return False

    def validate_column_names(self, column_names_query, table_name):
        """
        Validates if all column names provided in the list exist in the specified table.

        Parameters:
        - column_names_query (list of str): List of column names to check.
        - table_name (str): The table to check against.

        Returns:
        - CustomException: If any column does not exist in the table.
        """
        for column_name in column_names_query:
            if not self.column_exists_in_table_name(column_name, table_name):
                raise CustomException(Message.get_message(Message.INSERT_COLUMN_EXISTENCE_ERROR, column_name))

    def get_column_data_type(self, schema_str, column_name):
        """
        Retrieves the data type of a specified column from a schema string.

        Parameters:
        - schema_str (str): The schema string of the table.
        - column_name (str): The name of the column whose data type is to be retrieved.

        Returns:
        - str or None: The data type of the specified column if it is found in the schema string (ie. int, char(#), data)
                        or None if the column does not exist.
        """
        column_definitions = schema_str.split("|")[0].split(";")
        for col_def in column_definitions:
            col_name, col_type, _, _ = col_def.split(":")
            if col_name == column_name:
                return col_type
        return None

    # Helper Functions Handling Primary and Foreign Keys 
    def get_primary_keys(self, table_name):
        """
        Retrieves the list of primary key column names for a specified table from the database schema.

        Parameters:
        - table_name (str): The name of the table whose primary keys are to be retrieved.

        Returns:
        - list of str: A list of string representing the primary keys of the table.

        Note:
        This function assumes that the schema string for the table is correctly formatted and includes a section for primary keys prefixed with 'PK:'.
        """
        schema_str = self.get_table_schema(table_name)
        
        primary_keys = schema_str.split("|")[1].lstrip("PK:").split(',')

        if primary_keys == ['']:
            return []
        return primary_keys
    
    # Helper Functions Handling Primary and Foreign Keys 
    def get_foreign_keys(self, table_name):
        """
        Retrieves the list of foreign key column names for a specified table from the database schema.

        Parameters:
        - table_name (str): The name of the table whose foreign keys are to be retrieved.

        Returns:
        - list of str: A list of string representing the foreign keys of the table.

        Note:
        This function assumes that the schema string for the table is correctly formatted and includes a section for foreign keys prefixed with 'FK:'.
        """
        schema_str = self.get_table_schema(table_name)
        foreign_keys = schema_str.split("|")[2].lstrip("FK:").split(";")
        if foreign_keys == ['']:
            return []
        return foreign_keys
    
    def foreign_key_is_valid(self, foreign_key, foreign_key_definition_list):
        """
        Check if column name in list of foreign_key definitions

        Parameters:
        - foreign_key (str): column name of foreign key being checked
        - foreign_key_definition_list (list of str): list of foreign key definitions to search foreign_key against.
        
        Returns:
        - bool: True if foreign_key is in list, 
                False otherwise.
        """
        for fk_def in foreign_key_definition_list:
            fk_def_unpack = fk_def.split(":")[0].split(",")
            if foreign_key in fk_def_unpack:
                return True
        return False

    def find_referencing_tables(self, table_name):
        """
        Retrieves a list of tables that have foreign keys referencing the specified table.

        Parameters:
        - table_name (str): The name of the table for which to find referencing tables.

        Returns:
        - list of str: A list containing the names of all tables that reference the specified table through foreign keys.

        Note:
        - Assumed that the schema of each table is properly formatted and includes a section for foreign keys prefixed with 'FK:' 
        - Assumed that the 'FK:' section contains foreign key details in the format 'key_name:ref_table_name:key_columns'.
        """
        referencing_tables = []
        for potential_referrer in self.db.get_tables():  # Assume this returns a list of all table names
            schema_str = self.get_table_schema(potential_referrer)
            if schema_str:
                foreign_keys = schema_str.split("|")[2].lstrip("FK:").split(";")
                for fk in foreign_keys:
                    # Safely unpack the foreign key information assuming it's well-formed
                    if fk.count(':') == 2:  # Check that the foreign key string is in the expected format
                        _, ref_table_name, _ = fk.split(":")
                        if ref_table_name == table_name: 
                            referencing_tables.append(potential_referrer)
        return referencing_tables

    # SQL QUERY FUNCTIONS
    def create_table_query(self, items):
        """ CREATE TABLE """
        table_name = items[2].children[0].lower()

        if self.table_name_exists(table_name):
            raise CustomException(Message.get_message(Message.TABLE_EXISTENCE_ERROR))

        # Initialize schema to save attributes (name and type) & sets to collect PRI/FOR key information
        schema = []
        primary_keys = []
        foreign_keys = set()

        # Process column definitions
        column_definition_iter = items[3].find_data("column_definition")
        for column_definition in column_definition_iter:
            
            # Extract column_name
            column_name = str(column_definition.children[0].children[0]).lower()

            # Extract data_type
            data_type_tokens = [str(child) for child in column_definition.children[1].children] # Handling data types with parentheses and parameters such as char(5)
            data_type = "".join(data_type_tokens).lower() # The data type (ex. "char(10))"
            
            # Check for char type and validate length
            if "char" in data_type:
                char_length = int(data_type_tokens[2])  # This might need adjustment based on your parsing
                if char_length < 1:
                    raise CustomException(Message.get_message(Message.CHAR_LENGTH_ERROR))

            # Extract nullable 여부 information
            is_nullable = "Y" if column_definition.children[3] is None else "N"

            # Check for duplicate column name before proceeding
            if self.column_exists_in_schema(column_name, schema):
                raise CustomException(Message.get_message(Message.DUPLICATE_COLUMN_DEF_ERROR))
    
            schema.append({"name": column_name, "type": data_type, "nullable": is_nullable, "key": ""})

        # Extract primary key type
        primary_key_iter = items[3].find_data("primary_key_constraint")
        # If there is more than one line defining primary key constraints, raise error
        if len(list(primary_key_iter)) > 1:
            raise CustomException(Message.get_message(Message.DUPLICATE_PRIMARY_KEY_DEF_ERROR))

        # Regenerate iterator to process primary keys
        primary_key_iter = items[3].find_data("primary_key_constraint")
        for primary_key_definition in primary_key_iter:
            for primary_column_name_definition in primary_key_definition.find_data("column_name"):
                # Extract primary column name
                primary_column_name = primary_column_name_definition.children[0].lower()

                # Check existence of column_name before proceeding
                if not self.column_exists_in_schema(primary_column_name, schema):
                    raise CustomException(Message.get_message(Message.NON_EXISTING_COLUMN_DEF_ERROR, primary_column_name))
                
                primary_keys.append(primary_column_name)

        # Extract foreign key types
        foreign_key_iter = items[3].find_data("referential_constraint")
        # Iterate through each foreign key definition
        for foreign_key_definition in foreign_key_iter:
            # Extract table_name of referenced table
            referenced_table_name = foreign_key_definition.children[4].children[0].lower()

            # Check if foreign key references its own table
            if referenced_table_name == table_name:
                raise CustomException(Message.get_message(Message.REFERENCE_TABLE_SELF_ERROR))

            # Check existance of reference table before proceeding
            if not self.table_name_exists(referenced_table_name):
                raise CustomException(Message.get_message(Message.REFERENCE_TABLE_EXISTENCE_ERROR))

            # Retrieve schema for the referenced table
            referenced_schema_str = self.get_table_schema(referenced_table_name)

            #Initialize lists to store foreign key and referenced key names
            foreign_key_list = []
            referenced_key_list = []

            # Extract foreign_keys (individual column names) of referencing table
            for column_def in foreign_key_definition.children[2].children:
                if isinstance(column_def, Tree): # Excludes the PL and PR
                    foreign_column_name = column_def.children[0].lower()
                   
                    # Check existence of referencing foreign key before proceeding
                    if not self.column_exists_in_schema(foreign_column_name, schema):
                        raise CustomException(Message.get_message(Message.NON_EXISTING_COLUMN_DEF_ERROR, foreign_column_name))
                    foreign_key_list.append(foreign_column_name)
            
            # Extract referenced_keys (primary key) of referenced table
            for column_def in foreign_key_definition.children[5].children:
                if isinstance(column_def, Tree): 
                    referenced_column_name = column_def.children[0].lower()
                    
                    # Check existence of column name in referenced table before proceeding 
                    if not self.column_exists_in_table_name(referenced_column_name, referenced_table_name):
                        raise CustomException(Message.get_message(Message.REFERENCE_COLUMN_EXISTENCE_ERROR))
                   
                    referenced_key_list.append(referenced_column_name)
            
            # For each foreign key, check the data type matches with the corresponding primary key in the referenced table
            for fk_column, ref_column in zip(foreign_key_list, referenced_key_list):
                fk_type = next((col['type'] for col in schema if col['name'] == fk_column), None)
                ref_type = self.get_column_data_type(referenced_schema_str, ref_column)
                if fk_type != ref_type:
                    raise CustomException(Message.get_message(Message.REFERENCE_TYPE_ERROR))

            # Join column names with "," in composite foreign keys
            foreign_key = ",".join(foreign_key_list) 
            referenced_key = ",".join(referenced_key_list)

            # Check if foreign key references primary key of referenced table (accounts for subset)
            referenced_pk = ",".join(self.get_primary_keys(referenced_table_name))
            if referenced_key != referenced_pk: 
                raise CustomException(Message.get_message(Message.REFERENCE_NON_PRIMARY_KEY_ERROR))

            if len(foreign_key_list) != len(referenced_key_list):
                raise CustomException(Message.get_message(Message.REFERENCE_COLUMN_COUNT_MISMATCH_ERROR))
            
            foreign_keys.add(f"{foreign_key}:{referenced_table_name}:{referenced_key}") # Ex. "id,age,s_fname:person:id,age,fname"

        # Update schema list with key information
        for column in schema:
            column_keys = []
            if column["name"] in primary_keys:
                column_keys.append("PRI")
                column["nullable"] = "N"
            if self.foreign_key_is_valid(column["name"], foreign_keys): # Searches column name in list of foreign_key definitions
                column_keys.append("FOR")

            column["key"] = "/".join(column_keys) 
        
        # Format schema and add to database
        pk_enc = ",".join(primary_keys) 
        fk_enc = ";".join(foreign_keys)
        
        # Format Schema string to add into database
        columns_enc = ";".join([f"{col["name"]}:{col["type"]}:{col["nullable"]}:{col["key"]}" for col in schema])
        schema_enc  = f"{columns_enc}|PK:{pk_enc}|FK:{fk_enc}"

        self.db.insert_table(table_name, schema_enc)
        
        print(f"{PROMPT}'{table_name}' table is created") # CreateTableSuccess(#tableName)

    def drop_table_query(self, items):
        """ DROP TABLE """
        table_name = items[2].children[0].lower()
        
        # Check if table exists
        if not self.table_name_exists(table_name):
            raise CustomException(Message.get_message(Message.NO_SUCH_TABLE))
        
        # Check for foreign key referencing tables
        referencing_tables = self.find_referencing_tables(table_name)
        if referencing_tables:
            raise CustomException(Message.get_message(Message.DROP_REFERENCED_TABLE_ERROR, table_name))

        # Drop table
        self.db.drop_table(table_name) 

        print(f"'{table_name}' table is dropped")

        
    def explain_query(self, items):
        """ EXPLAIN """
        self.describe_query(items)

    def desc_query(self, items):
        """ DESC """
        self.describe_query(items)

    def describe_query(self, items):
        """ DESCRIBE"""
        table_name = items[1].children[0].lower() 

        # Check existence of table name before proceeding
        if not self.table_name_exists(table_name):
            raise CustomException(Message.get_message(Message.NO_SUCH_TABLE))

        # Receive table data from database
        schema_str = self.get_table_schema(table_name)

        # Print 
        print("-------------------------------------------------")
        print(f"table_name [{table_name}]")
        print("column_name\ttype\tnull\tkey")
        column_definitions = [column_def.split(":") for column_def in schema_str.split("|")[0].split(';')]
        for column_unpacked in column_definitions:
            column_name, column_type, is_nullable, key_type = column_unpacked
            print(f"{column_name}\t{column_type}\t{is_nullable}\t{key_type}")
        print("-------------------------------------------------")


    def select_query(self, items):
        """ SELECT """
    
        # Extract table names from FROM clause
        from_table_names =[table_name.children[0].lower() for table_name in items[2].children[0].find_data("table_name")] 
        
        for table_name in from_table_names:
            # Check if table exists before proceeding
            if not self.table_name_exists(table_name):
                raise CustomException(Message.get_message(Message.SELECT_TABLE_EXISTENCE_ERROR, table_name))
        
        # Get column details from SELECT clause
        select_list = items[1]  # Assuming this holds the select list
        select_list_columns = []  # Holds tuples of (column_name, table_name if specified)
        select_list_tables = [] # Holds all table names appearing in select list (SELECT lectures.id, student.name -> ['lectures', 'student'])
        for selected_column in select_list.find_data("selected_column"):
            table_name = selected_column.children[0].children[0].value.lower() if isinstance(selected_column.children[0], Tree) else None
            column_name = selected_column.children[1].children[0].value.lower()
            select_list_columns.append((table_name, column_name))

        # Map columns to tables and check for ambiguity
        select_column_table_map = []
        for specified_table, column in select_list_columns:
            if specified_table:
                # Check if table name exists
                if not self.table_name_exists(specified_table):
                    raise CustomException(Message.get_message(Message.SELECT_TABLE_EXISTENCE_ERROR, specified_table))
                
                # Direct mapping if table is specified
                if not self.column_exists_in_table_name(column, specified_table):
                    raise CustomException(Message.get_message(Message.SELECT_COLUMN_RESOLVE_ERROR, column))
                
                select_column_table_map.append((column, specified_table))
                select_list_tables.append(specified_table)

            else:
                # Find all tables that contain the column if no table is specified
                found_tables = [table for table in from_table_names if self.column_exists_in_table_name(column, table)]
                if len(found_tables) > 1:
                   raise CustomException(Message.get_message(Message.SELECT_COLUMN_RESOLVE_ERROR, column))
                elif not found_tables:
                    raise CustomException(Message.get_message(Message.SELECT_COLUMN_RESOLVE_ERROR, column))
                select_column_table_map.append((column, found_tables[0]))
            
        # Perform cartesian product from table in FROM clause
        initial_records, all_column_names = self.cartesian_product(from_table_names)

        if len(select_list_columns) == 0:
            # Select list non provided (SELECT *)
            column_names = all_column_names
        else:
            # Select list provided
            column_names = [f"{table}.{column}" for column, table in select_column_table_map]

        # Raise error if table in select list doesn't exist in FROM clause
        unexisting_tables = set(select_list_tables) - set(from_table_names)
        if len(unexisting_tables) > 0:
            raise CustomException(Message.get_message(Message.SELECT_TABLE_EXISTENCE_ERROR, list(unexisting_tables)[0]))

        # # Check if there's a WHERE clause
        where_clause = items[2].children[1]
        if where_clause is None:
            # No WHERE clause provided, select all records
            self.print_select_results(column_names, initial_records)
        else:
            conditions = self.extract_conditions(where_clause)
            
            for condition in [conditions[0], conditions[2]]:
                if condition is not None:
                    self.validate_condition(condition, from_table_names)

            # Select records matching the conditions
            selected_records = []
            for record in initial_records:
                if self.evaluate_conditions(record, conditions):
                    selected_records.append(record)

            self.print_select_results(column_names, selected_records)

    def cartesian_product(self, table_names):
        """
        Generate the Cartesian product of multiple tables.

        This function takes a list of table names and returns the Cartesian product of the records
        in these tables. It combines each record from the first table with each record from the 
        subsequent tables, creating a new set of combined records.

        Parameters:
        table_names (list of str): A list of table names for which the Cartesian product is to be generated.

        Returns:
        tuple:
            - result (list of dict): A list of dictionaries where each dictionary represents a combined 
            record from the Cartesian product of the input tables. The keys in the dictionary are 
            formatted as 'table_name.column_name' to avoid any naming conflicts.
            - column_names (list of str): A list of column names for the resulting records, each prefixed 
            with the table name to maintain uniqueness (formatted as 'table_name.column').
        """

        # Start with the records from the first table
        result = [{f"{table_names[0]}.{key}": value for key, value in record.items()} for record in self.db.retrieve_records(table_names[0])]
        
        # Collect column names from schema 
        column_names = [f"{table_names[0]}.{column}" for column in self.get_table_column_names(table_names[0])]

        # Loop through the other tables and form the product
        for table in table_names[1:]:
            new_result = []
            additional_column_names = [f"{table}.{column}" for column in self.get_table_column_names(table)]
            column_names.extend(additional_column_names)
            for record1 in result:
                for record2 in self.db.retrieve_records(table):
                    prefixed_record2 = {f"{table}.{key}": value for key, value in record2.items()}
                    new_result.append({**record1, **prefixed_record2})  # Merge dictionaries
            result = new_result
        
        return result, column_names

    def print_select_results(self, column_names, records):
        # Print header
        print("+--------------------------------------+") 
        column_name_formatted = "\t|".join([column for column in column_names if '#' not in column])
        print("|" + column_name_formatted + "\t|")
        print("+--------------------------------------+") 

        # Print each record
        for record in records:
            record_formatted = "\t|".join([record[column] for column in column_names if '#' not in record[column]]) 
            print("|" + record_formatted + "\t|")

        # Print footer
        print("+--------------------------------------+")


    def show_tables_query(self, items): 
        """ SHOW TABLES """
        tables = self.db.get_tables()
        print("------------------------")
        for table in tables:
            print(table)
        print("------------------------")
    
    def delete_query(self, items):
        """ DELETE """
        table_name = items[2].children[0].lower()

        # Check existance of table before proceeding
        if not self.table_name_exists(table_name):
            raise CustomException(Message.get_message(Message.NO_SUCH_TABLE))

        # Retrieve all records from the table
        initial_records = [{f"{table_name}.{key}": value for key, value in record.items()} for record in self.db.retrieve_records(table_name)]
        records_to_delete = []
        deleted_count = 0

        # Check if there's a WHERE clause
        where_clause = items[3]
        if where_clause is None:
            # No WHERE clause provided, delete all records
            records_to_delete = initial_records
            deleted_count = len(initial_records)  # All records will be deleted
        else:
            # WHERE clause provided
            # Extract conditions from WHERE clause
            conditions = self.extract_conditions(where_clause)
            
            # Validate conditions
            for condition in [conditions[0], conditions[2]]:
                if condition is not None:
                    self.validate_condition(condition, [table_name]) 

            # Evaluate conditions and Delete records matching the conditions
            for record in initial_records:
                if self.evaluate_conditions(record, conditions):
                    records_to_delete.append(record)
                    deleted_count += 1

        # Check if any record to delete is referenced as foreign key in another table
        foreign_key_referencing_records = self.get_foreign_key_referencing_records(table_name, records_to_delete)
        if len(foreign_key_referencing_records) > 0:
            raise CustomException(Message.get_message(Message.DELETE_REFERENTIAL_INTEGRITY_PASSED, deleted_count))

        # Proceed with deletion if all checks passed
        for record in records_to_delete:
            self.db.delete_record(table_name, record)
        
        print(f"{PROMPT}{deleted_count} row(s) deleted") # DeleteResult(#count)

    def extract_conditions(self, where_node):
        """
        Extracts conditions from a WHERE clause.

        This function processes the WHERE clause of a SQL query and extracts the conditions, 
        which can be either a single condition or a combination of conditions connected by 
        AND/OR operators.

        Parameters:
        where_node (Tree): The parsed WHERE clause node from the SQL query.

        Returns:
        tuple: A tuple representing the conditions extracted from the WHERE clause. The structure 
            of the tuple is (condition1, operator, condition2) where operator can be AND/OR/None.
        """
        bool_expr = where_node.children[1]
        boolean_terms = list(bool_expr.find_data("boolean_term"))
        boolean_terms_cnt = len(boolean_terms)
        boolean_factors = []
        condition_cnt = 0

        if boolean_terms_cnt == 1:
            # 1 condition / 2 conditions AND
            for boolean_factor in bool_expr.find_data("boolean_factor"):
                condition = self.extract_boolean_factor(boolean_factor)
                boolean_factors.append(condition)
                condition_cnt = condition_cnt + 1
            
            if condition_cnt == 1:
                # 1 condition
                conditions = (boolean_factors[0], None, None) 
            else:
                # 2 conditions AND
                conditions = (boolean_factors[0], AND, boolean_factors[1])
        else:
            # 2 conditions OR
            for boolean_factor in bool_expr.find_data("boolean_factor"):
                condition = self.extract_boolean_factor(boolean_factor)
                boolean_factors.append(condition)
                condition_cnt = condition_cnt + 1
            
            conditions = (boolean_factors[0], OR, boolean_factors[1])

        return conditions

    def validate_condition(self, condition, table_names):
        """
        Validates a condition in a WHERE clause.

        This function validates the condition extracted from a WHERE clause, ensuring that 
        the table and column references are correct and the comparison is valid.

        Parameters:
        condition (dict): The condition to validate.
        table_names (list of str): List of table names referenced in the query.

        Raises:
        CustomException: If there is an error with the table/column references or the comparison.
        """
        predicate_condition_type = condition["type"]
        
        if predicate_condition_type == "comparison_predicate":
            # Comparison predicate
            left_operand = condition["predicate"]["left_operand"]
            right_operand = condition["predicate"]["right_operand"]
            operands = [left_operand, right_operand]
            operator = condition["predicate"]["comp_op"]

            column_reference_operands = [operand for operand in operands if len(operand) >= 2]
            comparable_value_operands = [operand for operand in operands if operand not in column_reference_operands]
        else:
            # Null predicate
            left_operand = condition["predicate"]["left_operand"]
            operator = condition["predicate"]["comp_op"]
            column_reference_operands = [left_operand]

        # Check whether referrenced table name appears in FROM clause
        for column_reference_operand in column_reference_operands:
            # Store operand type
            column_reference_operand["operand_type"] = "column_reference"
            
            # Check table_name presence in FROM clause
            predicate_table_name = column_reference_operand["table_name"]
            predicate_column_name = column_reference_operand["column_name"]
            if predicate_table_name is not None and predicate_table_name not in table_names: 
                raise CustomException(Message.get_message(Message.WHERE_TABLE_NOT_SPECIFIED))

            # Validate column name exists in one of the tables.
            tables_containing_column = [table for table in table_names if self.column_exists_in_table_name(predicate_column_name, table)]
            if len(tables_containing_column) == 0:
                # No tables containing column_name
                raise CustomException(Message.get_message(Message.WHERE_COLUMN_NOT_EXIST))
            elif len(tables_containing_column) == 1:
                # Exactly one table containing column_name
                table_schema = self.get_table_schema(tables_containing_column[0])
                column_reference_operand["data_type"] = self.get_column_data_type(table_schema, predicate_column_name)
            else:
                # If more than one table contains the column name, predicate table_name must be given
                if predicate_table_name is None:
                    raise CustomException(Message.get_message(Message.WHERE_AMBIGUOUS_REFERENCE))
                else:
                    # Get column data type from the specified table_name
                    table_schema = self.get_table_schema(predicate_table_name)
                    column_reference_operand["data_type"] = self.get_column_data_type(table_schema, predicate_column_name)

        # Perform operands validation for comparison_predicate conditions
        if predicate_condition_type == "comparison_predicate":

            # Extract data types for comparable value operands as well
            for comparable_value_operand in comparable_value_operands:
                comparable_value_operand["data_type"] = self.get_comparable_value_data_type(comparable_value_operand["comparable_value"])
                comparable_value_operand["operand_type"] = "comparable_value"
            # Validate operation data types
            if not self.data_type_matches(left_operand["data_type"], right_operand["data_type"]):
                raise CustomException(Message.get_message(Message.WHERE_INCOMPARABLE_ERROR))
            elif not self.valid_operator(operator, left_operand["data_type"]):
                raise CustomException(Message.get_message(Message.WHERE_INCOMPARABLE_ERROR))

    def data_type_matches(self, left_operand_data_type, right_operand_data_type):
        return left_operand_data_type.startswith("char") and right_operand_data_type.startswith("char") or left_operand_data_type == right_operand_data_type
    
    def valid_operator(self, comp_op, operand_data_type):
        if operand_data_type == INT or operand_data_type == DATE:
            return comp_op in (GREATER_THAN, LESS_THAN, EQUAL, NOT_EQUAL, GREATER_OR_EQUAL, LESS_OR_EQUAL)
        elif operand_data_type.startswith("char"):
            return comp_op in (EQUAL, NOT_EQUAL)
        else:
            return comp_op in ("is null", "is not null") 

    def extract_boolean_factor(self, boolean_factor_node):
        """
        Extracts a boolean factor from a parsed WHERE clause node.

        This function processes a boolean factor node and extracts the condition, 
        which can be either a comparison or null predicate.

        Parameters:
        boolean_factor_node (Tree): The parsed boolean factor node from the WHERE clause.

        Returns:
        dict: A dictionary representing the extracted condition.
        """
        condition = dict()

        condition["is_not"] = False if boolean_factor_node.children[0] is None else True
        
        predicate = boolean_factor_node.children[1].children[0].children[0] 

        condition["predicate"] = dict()
        if predicate.data == "comparison_predicate":
            # comparison_predicate 
            condition["type"] = "comparison_predicate"
            
            condition["predicate"]["left_operand"] = self.extract_operand(predicate.children[0])
            condition["predicate"]["comp_op"] = predicate.children[1].children[0].value
            condition["predicate"]["right_operand"] = self.extract_operand(predicate.children[2])
        else: 
            # null_predicate
            condition["type"] = "null_predicate"
            table_name = predicate.children[0].children[0].value.lower() if isinstance(predicate.children[0], Tree) else None
            column_name = predicate.children[1].children[0].value.lower()
            condition["predicate"]["left_operand"] = {"table_name": table_name, "column_name": column_name}
            condition["predicate"]["comp_op"] = "is null" if predicate.children[2].children[1] is None else "is not null"

        return condition

    def extract_operand(self, comp_operand_node):
        """
        Extracts an operand from a parsed comparison predicate node.

        This function processes a comparison operand node and extracts the operand, 
        which can be either a column reference or a comparable value.

        Parameters:
        comp_operand_node (Tree): The parsed comparison operand node.

        Returns:
        dict: A dictionary representing the extracted operand.
        """
        comp_operand = dict()
        if len(comp_operand_node.children) == 2:
            # Case 1: [table_name "."] column_name
            comp_operand["table_name"] = comp_operand_node.children[0].children[0].value.lower() if isinstance(comp_operand_node.children[0], Tree) else None
            comp_operand["column_name"] = comp_operand_node.children[1].children[0].value.lower()
        elif len(comp_operand_node.children) == 1:
            # Case 2: comparable_value
            comp_operand["comparable_value"] = comp_operand_node.children[0].children[0].value.lower()

        return comp_operand
    
    def get_comparable_value_data_type(self, comparable_value):
        try: 
            int(comparable_value)
            return INT
        except ValueError:
            if re.match(r'^\d{4}-\d{2}-\d{2}$', comparable_value):
                return DATE
            elif ( (comparable_value.startswith("'") and comparable_value.endswith("'")) or \
                    (comparable_value.startswith('"') and comparable_value.endswith('"')) ):
                return CHAR

    def evaluate_conditions(self, record, condition):
        """
        Evaluates conditions against a record.

        This function checks if a record meets the conditions specified in the WHERE clause.

        Parameters:
        record (dict): The record to evaluate.
        condition (tuple): The conditions extracted from the WHERE clause.

        Returns:
        bool: True if the record meets all the conditions, False otherwise.
        """
        results = []  
        # for condition in conditions:
        if condition[1] is None:
            # Single condition evaluation
            results.append(self.evaluate_single_condition(record, condition[0]))
        elif condition[1].lower() in [AND, OR]:
            # Evaluate the two conditions
            cond1_result = self.evaluate_single_condition(record, condition[0])
            cond2_result = self.evaluate_single_condition(record, condition[2])
            if condition[1].lower() == AND:
                results.append(cond1_result and cond2_result)
            elif condition[1].lower() == OR:
                results.append(cond1_result or cond2_result)
        return all(results)
    
    def evaluate_single_condition(self, record, condition):
        """
        Evaluates a single condition against a record.

        This function checks if a record meets a single condition from the WHERE clause.

        Parameters:
        record (dict): The record to evaluate.
        condition (dict): The single condition to evaluate.

        Returns:
        bool: True if the record meets the condition, False otherwise.
        """    
        # Extract column, operator and value from WHERE condition
        condition_negation = condition["is_not"]
        condition_type = condition["type"]

        left_operand_value = ""
        right_operand_value = ""

        if condition_type == "comparison_predicate":
            # Comparison predicate
            left_operand_value = self.extract_record_value(record, condition["predicate"]["left_operand"])
            right_operand_value = self.extract_record_value(record, condition["predicate"]["right_operand"])
            
            operator = condition["predicate"]["comp_op"]
        else:
            # Null predicate
            left_operand_value = self.extract_record_value(record, condition["predicate"]["left_operand"])
            
            operator = condition["predicate"]["comp_op"]

        # Evaluate comparison based on the operator
        if condition_type == "comparison_predicate":
            if NULL in [left_operand_value, right_operand_value]:
                # Any comparison with NULL is UNKNOWN, thus returns False
                result = False
            elif operator == '=':
                result = left_operand_value == right_operand_value
            elif operator == '!=':
                result = left_operand_value != right_operand_value
            elif operator == '>':
                result = left_operand_value > right_operand_value
            elif operator == '<':
                result = left_operand_value < right_operand_value
            elif operator == '>=':
                result = left_operand_value >= right_operand_value
            elif operator == '<=':
                result = left_operand_value <= right_operand_value
        else:
            if operator == "is null":
                result = left_operand_value == NULL
            elif operator == "is not null":
                result = left_operand_value != NULL
        
        if condition_negation:
            return not result
        return result

    def extract_record_value(self, record, operand):
        """
        Extracts the value of an operand from a record.

        This function retrieves the value of an operand, which can be either a column reference 
        or a comparable value, from the given record.

        Parameters:
        record (dict): The record from which to extract the value.
        operand (dict): The operand to extract.

        Returns:
        any: The extracted value, with appropriate type conversion.
        """
        # Handle cases where table name is provided and when it is not.
        if operand["operand_type"] == "column_reference":
            # Column reference value
            table_name = operand["table_name"]
            column_name = operand["column_name"]
            if table_name:
                # Table name is provided, use it directly.
                column_key = f"{table_name}.{column_name}".lower()
            else:
                # Table name not provided, find any matching key.
                column_key = next((key for key in record if key.endswith(f".{column_name}")), None) # Assumes that column name is unique when table name isn't specified.

            # Retrieve the record value using the constructed or found key.
            record_value = record.get(column_key)
        else:
            # Comparable value
            record_value = operand["comparable_value"].strip('\'"').lower()

        # Type conversion (if null, return null)
        data_type = operand["data_type"]
        if record_value == NULL:
            pass
        elif data_type == INT:
            record_value = int(record_value)
        elif data_type == DATE:
            record_value = datetime.strptime(record_value, '%Y-%m-%d').date()

        return record_value

    def get_foreign_key_referencing_records(self, table_name, records):
        """
        Checks if the record is referenced by any foreign key in other tables.

        This function checks if any of the records in the given table are referenced 
        by foreign keys in other tables.

        Parameters:
        table_name (str): The name of the table containing the records to check.
        records (list of dict): The records to check for foreign key references.

        Returns:
        list of dict: A list of records that reference the given records via foreign keys.
        """
        referencing_tables = self.find_referencing_tables(table_name)
        foreign_key_referencing_records = []
        for record in records:
            record_to_check = {key.split('.')[-1]: value for key, value in record.items()}
            for referencing_table in referencing_tables:
                foreign_keys = self.get_foreign_keys(referencing_table)
                for fk in foreign_keys:
                    fk_column, referenced_table_name, referenced_column = fk.split(":")
                    if referenced_table_name == table_name:
                        query = {fk_column: record_to_check[referenced_column]}
                        referencing_record = self.db.retrieve_specific_pk_record(referencing_table, query)
                        foreign_key_referencing_records.extend(referencing_record)

        return foreign_key_referencing_records

    def insert_query(self, items):
        """ INSERT """
        table_name = items[2].children[0].lower()

        # Check existance of table before proceeding
        if not self.table_name_exists(table_name):
            raise CustomException(Message.get_message(Message.NO_SUCH_TABLE))

        # Get data type constraints from table schema
        schema_str = self.get_table_schema(table_name)
        schema = schema_str.split("|")[0].split(';')
        column_info = {col.split(":")[0]: [col.split(":")[1], col.split(":")[2]] for col in schema} # Creates dictionary with key: column, value: [data_type, nullable]

        # Extract column list if specified in query
        column_names_query = []
        if items[3] is not None:
            column_name_iter = items[3].find_data("column_name")
            column_names_query = [col.children[0].lower() for col in column_name_iter]

            # Validate the column names
            try:
                self.validate_column_names(column_names_query, table_name)
            except CustomException:
                raise 

            # Check if number of column names and number of values to be inserted match
            if len(column_names_query) != len(set(column_names_query)):
                raise CustomException(Message.get_message(Message.INSERT_TABLE_DUPLICATE_COLUMN_ERROR))

            # Check if all Primary Keys are included in the column list
            primary_keys_set = set(self.get_primary_keys(table_name))
            unincluded_pks_set = primary_keys_set - set(column_names_query)
            if len(unincluded_pks_set) > 0:
                raise CustomException(Message.get_message(Message.INSERT_COLUMN_NON_NULLABLE_ERROR, list(unincluded_pks_set)[0]))
        else:
            # If column list isn't specified in query, extract from schema
            column_names_query = [col.split(":")[0] for col in schema]

        # Initialize values with null (fills in ungiven values with null)
        row_values  = {col.split(":")[0]: "null" for col in schema}
        
        # Collect values
        values_list_iter = items[5].find_data("value")
        inserted_values_enum = enumerate(values_list_iter)
        inserted_values_list = list(inserted_values_enum)

        # Check whether number of columns and inserted values match before proceeding
        if len(column_names_query) != len(list(inserted_values_list)):
            raise CustomException(Message.get_message(Message.INSERT_TYPE_MISMATCH_ERROR))

        for idx, value in inserted_values_list:
            # Get corresponding column name and data type (needed for comparison)
            column_name = column_names_query[idx]
            column_data_type = column_info[column_name][0]
            column_is_nullable = column_info[column_name][1]

            # Check if the first child is a string and perform a case-insensitive match for "null"
            if isinstance(value.children[0], str) and value.children[0].lower() == "null":
                data_value = "null"
                if column_is_nullable != "Y":
                    raise CustomException(Message.get_message(Message.INSERT_COLUMN_NON_NULLABLE_ERROR, column_name))
           
            # Handle the case where the child is a Tree (ie. comparable value)
            elif isinstance(value.children[0], Tree):
                data_value = value.children[0].children[0].lower()

                # Validate data type between column an value to be inserted
                if column_data_type == "int":
                    try:
                        value_int_convert = int(data_value)
                    except ValueError:
                        raise CustomException(Message.get_message(Message.INSERT_TYPE_MISMATCH_ERROR))

                elif column_data_type == "date":
                    if not re.match(r'^\d{4}-\d{2}-\d{2}$', data_value):
                        raise CustomException(Message.get_message(Message.INSERT_TYPE_MISMATCH_ERROR))

                elif column_data_type.startswith('char'):
                    # Extract max length from the data type definition such as 'char(10)'
                    match = re.match(r'char\((\d+)\)', column_data_type)
                    max_length = int(match.group(1))

                    # If inserted value is string, truncate to max length
                    if (data_value.startswith("'") and data_value.endswith("'")) or \
                        (data_value.startswith('"') and data_value.endswith('"')):
                        data_value = data_value.strip('\'"')[:max_length] 
                    else:
                        raise CustomException(Message.get_message(Message.INSERT_TYPE_MISMATCH_ERROR))

            # Store inserted value for corresponding column name
            row_values[column_name] = data_value
        
        # Check for Primary Key Duplication (Optional)
        pk_column_list = self.get_primary_keys(table_name)
        if len(pk_column_list) > 0: # Skip if table doesn't have a Primary Key
            insert_pk_value_dict = {pk_column_name: row_values[pk_column_name] for pk_column_name in pk_column_list}
            if self.pk_value_exists(table_name, insert_pk_value_dict):
                raise CustomException(Message.get_message(Message.INSERT_DUPLICATE_PRIMARY_KEY_ERROR))

        # Verify foreign key constraints (Optional)
        self.verify_foreign_keys(table_name, row_values)

        # # Insert the row into the database
        self.db.insert_row(table_name, row_values)
        print(f"{PROMPT}1 row inserted") # InsertResult
    
    def pk_value_exists(self, table_name, query_pk_values_dict):
        """
        Check if the primary key value already exists in the table.

        Parameters:
        - table_name (str): The name of the table.
        - query_pk_values_dict (dict): The dict containing primary key and value to match.

        Returns:
        - bool: True if the primary key value exists, False otherwise.
        """
        existing_value = self.db.retrieve_specific_pk_record(table_name, query_pk_values_dict)
        return len(existing_value) != 0

    def verify_foreign_keys(self, table_name, row_values):
        """
        Verifies that foreign key values exist as primary keys in their respective referenced tables.

        Parameters:
        - table_name (str): The name of the table.
        - row_values (dict): The dictionary containing column names and their corresponding values.

        Raises:
        - CustomException: If a foreign key value does not exist as a primary key in the referenced table.
        """
        
        # Return details about foreign keys and their referenced tables
        foreign_keys_info_list = self.get_foreign_keys(table_name)  

        if len(foreign_keys_info_list) == 0:
            # Skip verification if table doesn't have any Foreign Keys
            return 
        
        for foreign_key_info in foreign_keys_info_list:
            fk_column_name, ref_table_name, ref_column_name = foreign_key_info.split(":") # foreign_key_info format: "id:lectures:id"
            fk_value = row_values[fk_column_name]
            query_fk_values_dict = {ref_column_name: fk_value}
            if len(self.db.retrieve_specific_pk_record(ref_table_name, query_fk_values_dict)) == 0:
                raise CustomException(Message.get_message(Message.INSERT_REFERENTIAL_INTEGRITY_ERROR))

    def update_query(self, items):
        print(PROMPT + "\'UPDATE\' requested")
    
    def EXIT(self, items):
        exit()

# Open and read grammar from grammar.lark 
with open('grammar.lark') as file:
    sql_parser = Lark(file.read(), start="command", lexer="basic")

# Function to parse each individual query
def parse_query(query, db):
    try:
        output = sql_parser.parse(query)
        myTransformer = MyTransformer(db)
        myTransformer.transform(output)
        return True # Parsing was successful
    except exceptions.UnexpectedInput:
        print(PROMPT + "Syntax error") # Syntax Error
        return False # Parsing failed
    except exceptions.VisitError as e:
        if isinstance(e.orig_exc, CustomException):
            print(PROMPT + e.orig_exc.message) # Handle custom error
        else:
            raise e           

#Debug function (personal use)
def debug(input):
    queries = input.split(";")[:-1]
    print("-------------DEBUG-------------")
    print("QUERIES LIST(length=" + str(len(queries)) +"): " + str(queries) + "\n") 
    
    print("--QUERIES--")
    queries_count = 0
    for q in queries:
            queries_count += 1
            print("query " + str(queries_count) + ": " + q + ";")            
    print("-----------")
    print("QUERIES COUNT: " + str(queries_count))
    print("-------------------------------")   

# Main Function
def main():

    # Create and open database
    myDB = Database('myDB.db')

    # Main loop for receiving user input           
    while True:
        # Receive initial input from user (either query sequence or first line of multiline query input)
        user_input = input(PROMPT) # "DB_2020-16634> "

        # Receive multiline input from user if initial input doesn't contain ";"
        multiline_user_input = [user_input]
        while not user_input.__contains__(";"):
            line = input()
            multiline_user_input.append(line)
            if line.__contains__(";"):
                break
        user_input = "\n".join(multiline_user_input) # Combine all input lines into a single string
        
        # Separate input string into list of queries based on ";" separator
        queries = user_input.split(";")

        # Iterate through each individual query and process it through the lark parser
        for q in queries[:-1]:
            success = parse_query(q + ";", myDB)  # Add ";" back for parsing, deleted from split function, pass database with query to parse
            if not success:
                break # Stop processing queries after a syntax error

    # Close database
    myDB.close()

if __name__ == "__main__":
	main()
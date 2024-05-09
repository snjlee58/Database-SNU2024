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
                raise CustomException(f"Insertion has failed: '{column_name}' does not exist") #InsertColumnExistenceError(#colName)


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
        
        primary_keys = schema_str.split("|")[1].lstrip("PK:")
        foreign_keys = schema_str.split("|")[2].lstrip("FK:").split(";")

        return primary_keys
    
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
                        if ref_table_name == table_name: #FIX: change to indexing
                            referencing_tables.append(potential_referrer)
        return referencing_tables

    # SQL QUERY FUNCTIONS
    def create_table_query(self, items):
        """ CREATE TABLE """
        table_name = items[2].children[0].lower()

        if self.table_name_exists(table_name):
            raise CustomException("Create table has failed: table with the same name already exists") # TableExistenceError

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
                    raise CustomException("Char length should be over 0") # CharLengthError

            # Extract nullable 여부 information
            is_nullable = "Y" if column_definition.children[3] is None else "N"

            # Check for duplicate column name before proceeding
            if self.column_exists_in_schema(column_name, schema):
               raise CustomException("Create table has failed: column definition is duplicated") # DuplicateColumnDefError 
    
            schema.append({"name": column_name, "type": data_type, "nullable": is_nullable, "key": ""})

        # Extract primary key type
        primary_key_iter = items[3].find_data("primary_key_constraint")
        # If there is more than one line defining primary key constraints, raise error
        if len(list(primary_key_iter)) > 1:
            raise CustomException("Create table has failed: primary key definition is duplicated") # DuplicatePrimaryKeyDefError

        # Regenerate iterator to process primary keys
        primary_key_iter = items[3].find_data("primary_key_constraint")
        for primary_key_definition in primary_key_iter:
            for primary_column_name_definition in primary_key_definition.find_data("column_name"):
                # Extract primary column name
                primary_column_name = primary_column_name_definition.children[0].lower()

                # Check existence of column_name before proceeding
                if not self.column_exists_in_schema(primary_column_name, schema):
                    raise CustomException(f"Create table has failed: '{primary_column_name}' does not exist in column definition") #NonExistingColumnDefError(#colName)
                
                primary_keys.append(primary_column_name)

        # Extract foreign key types
        foreign_key_iter = items[3].find_data("referential_constraint")
        # Iterate through each foreign key definition
        for foreign_key_definition in foreign_key_iter:
            # Extract table_name of referenced table
            referenced_table_name = foreign_key_definition.children[4].children[0].lower()

            # Check if foreign key references its own table
            if referenced_table_name == table_name:
                raise CustomException("Create table has failed: foreign key cannot reference its own table") #ReferenceTableSelfError

            # Check existance of reference table before proceeding
            if not self.table_name_exists(referenced_table_name):
                raise CustomException("Create table has failed: foreign key references non existing table") # ReferenceTableExistenceError

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
                        raise CustomException(f"Create table has failed: '{foreign_column_name}' does not exist in column definition") #NonExistingColumnDefError(#colName)

                    foreign_key_list.append(foreign_column_name)
            
            # Extract referenced_keys (primary key) of referenced table
            for column_def in foreign_key_definition.children[5].children:
                if isinstance(column_def, Tree): 
                    referenced_column_name = column_def.children[0].lower()
                    
                    # Check existence of column name in referenced table before proceeding 
                    if not self.column_exists_in_table_name(referenced_column_name, referenced_table_name):
                        raise CustomException("Create table has failed: foreign key references non existing column") # ReferenceColumnExistenceError
                   
                    referenced_key_list.append(referenced_column_name)
            
            # For each foreign key, check the data type matches with the corresponding primary key in the referenced table
            for fk_column, ref_column in zip(foreign_key_list, referenced_key_list):
                fk_type = next((col['type'] for col in schema if col['name'] == fk_column), None)
                ref_type = self.get_column_data_type(referenced_schema_str, ref_column)
                if fk_type != ref_type:
                    raise CustomException("Create table has failed: foreign key references wrong type") # ReferenceTypeError

            # Join column names with "," in composite foreign keys
            foreign_key = ",".join(foreign_key_list) 
            referenced_key = ",".join(referenced_key_list)

            # Check if foreign key references primary key of referenced table (accounts for subset)
            referenced_pk = self.get_primary_keys(referenced_table_name)
            if referenced_key != referenced_pk: 
                raise CustomException("Create table has failed: foreign key references non primary key column") # ReferenceNonPrimaryKeyError

            if len(foreign_key_list) != len(referenced_key_list):
                raise CustomException("Create table has failed: number of referencing columns does not match number of referenced columns") # ReferenceColumnCountMismatchError

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
            raise CustomException("No such table") # NoSuchTable Error
        
        # Check for foreign key referencing tables
        referencing_tables = self.find_referencing_tables(table_name)
        if referencing_tables:
            raise CustomException(f"Drop table has failed: '{table_name}' is referenced by other table") # DropReferencedTableError(#tableName)

        # Drop table
        self.db.drop_table(table_name) 
        print(f"{PROMPT}'{table_name}' table is dropped") # DropSuccess(#tableName)
        
        
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
            raise CustomException("No such table") # NoSuchTable

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
        referred_table_names =[table_name.children[0].lower() for table_name in items[2].children[0].find_data("table_name")] 
        
        for table_name in referred_table_names:
            # Check if table exists before proceeding
            if not self.table_name_exists(table_name):
                raise CustomException(f"Selection has failed: '{table_name}' does not exist") # SelectTableExistenceError(#tableName)
        
        # Get column details from SELECT clause
        select_list = items[1]  # Assuming this holds the select list
        selected_columns = []  # This will hold tuples of (column, table if specified)
        for selected_column in select_list.find_data("selected_column"):
            table_name = selected_column.children[0].children[0].value.lower() if isinstance(selected_column.children[0], Tree) else None
            column_name = selected_column.children[1].children[0].value.lower()
            selected_columns.append((table_name, column_name))

        # Map columns to tables and check for ambiguity
        select_column_table_map = []
        for specified_table, column in selected_columns:
            if specified_table:
                # Direct mapping if table is specified
                if not self.column_exists_in_table_name(column, specified_table):
                    raise CustomException(f"Selection has failed: fail to resolve '{column}'") # SelectColumnResolveError(#colName)
                select_column_table_map.append((column, specified_table))

            else:
                # Find all tables that contain the column if no table is specified
                found_tables = [table for table in referred_table_names if self.column_exists_in_table_name(column, table)]
                if len(found_tables) > 1:
                   raise CustomException(f"Selection has failed: fail to resolve '{column}'") # SelectColumnResolveError(#colName)
                elif not found_tables:
                    raise CustomException(f"Selection has failed: fail to resolve '{column}'") # SelectColumnResolveError(#colName)
                select_column_table_map.append((column, found_tables[0]))
            
        # Perform cartesian product from table in FROM clause
        initial_records, all_column_names = self.cartesian_product(referred_table_names)
        # print(initial_records) #DELETE
        # print(column_names) #DELETE

        if len(selected_columns) == 0:
            # Select list non provided (SELECT *)
            column_names = all_column_names
        else:
            # Select list provided
            column_names = [f"{table}.{column}" for column, table in select_column_table_map]

        # # Check if there's a WHERE clause
        where_clause = items[2].children[1]
        if where_clause is None:
            # No WHERE clause provided, select all records
            self.print_select_results(column_names, initial_records)
        else:
            conditions = self.extract_conditions(where_clause)
            
            for condition in [conditions[0], conditions[2]]:
                if condition is not None:
                    self.validate_condition(condition, referred_table_names)

            # Select records matching the conditions
            selected_records = []
            for record in initial_records:
                if self.evaluate_conditions(record, conditions):
                    selected_records.append(record)

            self.print_select_results(column_names, selected_records)

    def cartesian_product(self, table_names):
        """ Generate the Cartesian product of multiple tables. """

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
            raise CustomException("No such table") # NoSuchTable

        # Retrieve all records from the table
        initial_records = [{f"{table_name}.{key}": value for key, value in record.items()} for record in self.db.retrieve_records(table_name)]
        deleted_count = 0

        # Check if there's a WHERE clause
        where_clause = items[3]
        if where_clause is None:
            # No WHERE clause provided, delete all records
            deleted_count = len(initial_records)  # All records will be deleted
            self.db.delete_all_table_records(table_name)
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
                    self.db.delete_record(table_name, record)
                    deleted_count += 1

        print(f"{PROMPT}{deleted_count} row(s) deleted") # DeleteResult(#count)

    #TODO
    def extract_conditions(self, where_node):
        bool_expr = where_node.children[1]
        boolean_terms = list(bool_expr.find_data("boolean_term"))
        boolean_terms_cnt = len(boolean_terms)
        boolean_factors = []
        condition_cnt = 0
        if boolean_terms_cnt == 1:
            # 1 condition / 2 conditions AND
            for boolean_factor in bool_expr.find_data("boolean_factor"):
                # first_condition = self.extract_boolean_factor(boolean_terms[0].children[0])
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
                # first_condition = self.extract_boolean_factor(boolean_terms[0].children[0])
                condition = self.extract_boolean_factor(boolean_factor)
                boolean_factors.append(condition)
                condition_cnt = condition_cnt + 1
            
            conditions = (boolean_factors[0], OR, boolean_factors[1])

        print(f"conditions: {conditions}") #DELETE
        return conditions

    def validate_condition(self, condition, table_names):
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
                raise CustomException("Where clause trying to reference tables which are not specified") # WhereTableNotSpecified

            # Validate column name exists in one of the tables.
            tables_containing_column = [table for table in table_names if self.column_exists_in_table_name(predicate_column_name, table)]
            if len(tables_containing_column) == 0:
                # No tables containing column_name
                raise CustomException("Where clause trying to reference non existing column") # WhereColumnNotExist
            elif len(tables_containing_column) == 1:
                # Exactly one table containing column_name
                table_schema = self.get_table_schema(tables_containing_column[0])
                column_reference_operand["data_type"] = self.get_column_data_type(table_schema, predicate_column_name)
            else:
                # If more than one table contains the column name, predicate table_name must be given
                if predicate_table_name is None:
                    raise CustomException("Where clause contains ambiguous reference") # WhereAmbiguousReference
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
                print("operand data types dont match") #DELETE
                raise CustomException("Where clause trying to compare incomparable values") # WhereIncomparableError
            elif not self.valid_operator(operator, left_operand["data_type"]):
                print("invalid operator for operands") #DELETE
                raise CustomException("Where clause trying to compare incomparable values") # WhereIncomparableError
            print(f"operands: {operands}") #DELETE

    def data_type_matches(self, left_operand_data_type, right_operand_data_type):
        print(left_operand_data_type, right_operand_data_type) #DELETE
        return left_operand_data_type.startswith("char") and right_operand_data_type.startswith("char") or left_operand_data_type == right_operand_data_type
    
    def valid_operator(self, comp_op, operand_data_type):
        if operand_data_type == INT or operand_data_type == DATE:
            return comp_op in (GREATER_THAN, LESS_THAN, EQUAL, NOT_EQUAL, GREATER_OR_EQUAL, LESS_OR_EQUAL)
        elif operand_data_type.startswith("char"):
            return comp_op in (EQUAL, NOT_EQUAL)
        else:
            return comp_op in ("is null", "is not null") #FIX


    def extract_boolean_factor(self, boolean_factor_node):
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
            table_name = predicate.children[0].children[0].value if isinstance(predicate.children[0], Tree) else None
            column_name = predicate.children[1].children[0].value
            condition["predicate"]["left_operand"] = {"table_name": table_name, "column_name": column_name}
            condition["predicate"]["comp_op"] = "is null" if predicate.children[2].children[1] is None else "is not null"

        return condition

     # Function to handle operand that might be a column reference or a comparable value
    def extract_operand(self, comp_operand_node):
        comp_operand = dict()
        if len(comp_operand_node.children) == 2:
            # Case 1: [table_name "."] column_name
            comp_operand["table_name"] = comp_operand_node.children[0].children[0].value if isinstance(comp_operand_node.children[0], Tree) else None
            comp_operand["column_name"] = comp_operand_node.children[1].children[0].value
        elif len(comp_operand_node.children) == 1:
            # Case 2: comparable_value
            comp_operand["comparable_value"] = comp_operand_node.children[0].children[0].value

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
        print(f"evaluate_conditions results: {results}") #DELETE
        return all(results)
    
    def evaluate_single_condition(self, record, condition):     
        # Extract column, operator and value from WHERE condition
        print(record) #DELETE
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

        # print(f"column_key: {column_key}") #DELETE
        # print(f"record: {record}") #DELETE
        print(f"left_operand_value: {left_operand_value}") #DELETE
        print(f"right_operand_value: {right_operand_value}") #DELETE

        # Evaluate comparison based on the operator
        if operator == '=':
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
        elif operator == "is null":
            result = left_operand_value == NULL
        elif operator == "is not null":
            result = left_operand_value != NULL
        # Extend for other comparison operators
        
        print(f"result: {result}") #DELETE
        if condition_negation:
            return not result
        return result

    def extract_record_value(self, record, operand):
        # Handle cases where table name is provided and when it is not.
        print(operand) #DELETE
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

        # Type conversion
        data_type = operand["data_type"]
        if data_type == INT:
            record_value = int(record_value)
        elif data_type == DATE:
            record_value = datetime.strptime(record_value, '%Y-%m-%d').date()

        return record_value

    def insert_query(self, items):
        """ INSERT """
        table_name = items[2].children[0].lower()

        # Check existance of table before proceeding
        if not self.table_name_exists(table_name):
            raise CustomException("No such table") # NoSuchTable

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
                raise CustomException("Insert has failed: column name is duplicated") #InsertTableDuplicateColumnError
        else:
            # If column order isn't specified in query, use default order from schema
            column_names_query = [col.split(":")[0] for col in schema]

        # Initialize values with null (fills in ungiven values with null)
        row_values  = {col.split(":")[0]: "null" for col in schema}
        
        # Collect values
        values_list_iter = items[5].find_data("value")
        inserted_values_enum = enumerate(values_list_iter)
        inserted_values_list = list(inserted_values_enum)

        # Check whether number of columns and inserted values match before proceeding
        if len(column_names_query) != len(list(inserted_values_list)):
            raise CustomException("Insertion has failed: Types are not matched") #InsertTypeMismatchError

        for idx, value in inserted_values_list:
            # Get corresponding column name and data type (needed for comparison)
            column_name = column_names_query[idx]
            column_data_type = column_info[column_name][0]
            column_is_nullable = column_info[column_name][1]

            # Check if the first child is a string and perform a case-insensitive match for "null"
            if isinstance(value.children[0], str) and value.children[0].lower() == "null":
                data_value = "null"
                if column_is_nullable != "Y":
                    raise CustomException(f"Insertion has failed: '{column_name}' is not nullable") #InsertColumnNonNullableError(#colName)
           
            # Handle the case where the child is a Tree (ie. comparable value)
            elif isinstance(value.children[0], Tree):
                data_value = value.children[0].children[0].lower()

                # Validate data type between column an value to be inserted
                if column_data_type == "int":
                    try:
                        value_int_convert = int(data_value)
                    except ValueError:
                        raise CustomException("Insertion has failed: Types are not matched") #InsertTypeMismatchError

                elif column_data_type == "date":
                    if not re.match(r'^\d{4}-\d{2}-\d{2}$', data_value):
                        raise CustomException("Insertion has failed: Types are not matched") #InsertTypeMismatchError

                elif column_data_type.startswith('char'):
                    # Extract max length from the data type definition such as 'char(10)'
                    match = re.match(r'char\((\d+)\)', column_data_type)
                    max_length = int(match.group(1))

                    # If inserted value is string, truncate to max length
                    if (data_value.startswith("'") and data_value.endswith("'")) or \
                        (data_value.startswith('"') and data_value.endswith('"')):
                        data_value = data_value.strip('\'"')[:max_length] 
                    else:
                        raise CustomException("Insertion has failed: Types are not matched") #InsertTypeMismatchError

            # Store inserted value for corresponding column name
            row_values[column_name] = data_value

        # # Insert the row into the database
        self.db.insert_row(table_name, row_values)
        print(f"{PROMPT}1 row inserted") # InsertResult
    
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
        print(output.pretty()) #DELETE
        myTransformer = MyTransformer(db)
        myTransformer.transform(output)
        return True # Parsing was successful
    except exceptions.UnexpectedInput:
        print(PROMPT + "Syntax error")
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

        # Debug (personal use): outputs list of queries processed
        # debug(user_input) #DELETE

        # Iterate through each individual query and process it through the lark parser
        for q in queries[:-1]:
            success = parse_query(q + ";", myDB)  # Add ";" back for parsing, deleted from split function, pass database with query to parse
            if not success:
                break # Stop processing queries after a syntax error

    # Close database
    myDB.close()

if __name__ == "__main__":
	main()
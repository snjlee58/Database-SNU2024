from lark import Lark, Transformer, exceptions, Tree
from berkeleydb import db
from Database import *
from CustomException import *

# Input prompt
PROMPT = "DB_2020-16634> "

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

    def get_column_data_type(self, schema_str, column_name):
        """
        Retrieves the data type of a specified column from a schema string.

        Parameters:
        - schema_str (str): The schema string of the table.
        - column_name (str): The name of the column whose data type is to be retrieved.

        Returns:
        - str or None: The data type of the specified column if it is found in the schema string, or None if the column does not exist.
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
        # Extract table name
        table_name =  items[2].children[0].children[1].children[0].children[0].children[0].lower()
        
        # Check if table exists before proceeding
        if not self.table_name_exists(table_name):
            raise CustomException(f"Selection has failed: '{table_name}' does not exist") # SelectTableExistenceError(#tableName)
        
        # Retrieve schema and records from database
        schema = self.get_table_schema(table_name)
        records = self.db.retrieve_records(table_name)

        # Collect column names from schema 
        column_names = []
        for column_def in schema.split("|")[0].split(';'):
            column_name = column_def.split(":")[0]
            column_names.append(column_name)

        # Print header
        print("+--------------------------------------+") 
        column_name_formatted = "\t|".join([column for column in column_names])
        print("|" + column_name_formatted + "\t|")
        print("+--------------------------------------+") 

        # Print each record
        for record in records:
            record_formatted = "\t|".join([record[column] for column in column_names]) 
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
        print(PROMPT + "\'DELETE\' requested")
    
    def insert_query(self, items):
        """ INSERT """
        table_name = items[2].children[0].lower()

        # Check existance of table before proceeding
        if not self.table_name_exists(table_name):
            raise CustomException("No such table") # NoSuchTable

        # Get data type constraints from table schema
        schema_str = self.get_table_schema(table_name)
        schema = schema_str.split("|")[0].split(';')
        column_data_type = {col.split(":")[0]: col.split(":")[1] for col in schema} # Creates dictionary with key: column, value: data type

        # Extract column list if specified in query
        column_names_query = []
        if items[3] is not None:
            column_name_iter = items[3].find_data("column_name")
            column_names_query = [col.children[0].lower() for col in column_name_iter]
            if len(column_names_query) != len(set(column_names_query)):
                raise CustomException("Insert has failed: column name is duplicated") #InsertTableDuplicateColumnError
        else:
            # If column order isn't specified in query, use default order from schema
            column_names_query = [col.split(":")[0] for col in schema]

        # Collect values
        row_values  = dict()
        values_list_iter = items[5].find_data("value")
    
        for idx, value in enumerate(values_list_iter):
            if isinstance(value.children[0], str) and value.children[0].lower() == "null":
                # Check if the first child is a string and perform a case-insensitive match for "null"
                data_value = "null"
            elif isinstance(value.children[0], Tree):
                # Handle the case where the child is a Tree (ie. comparable value)
                data_value = value.children[0].children[0].lower()

            column_name = column_names_query[idx]
            data_type = column_data_type[column_name]

            # For char data type values: check length, truncate if longer than constraint
            if data_type.startswith('char'):
                # Parse the length from the data type string, example: 'char(10)'
                length = int(data_type[data_type.find('(')+1:data_type.find(')')])
                # Truncate the data value if it exceeds the specified length
                data_value = data_value.strip('\'"')[:length]

            row_values[column_name] = data_value

        # # Insert the row into the database
        self.db.insert_row(table_name, row_values)
        print(f"{PROMPT}The row is inserted") # InsertResult
    
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
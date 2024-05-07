from berkeleydb import db
import json

SCHEMA_KEY_PREFIX = "##"
COUNTER_KEY = "###counter"

class Database:
    def __init__(self, db_filename):
        self.db_filename = db_filename
        self.db = db.DB()
        self.db.open(self.db_filename, dbtype=db.DB_HASH, flags=db.DB_CREATE)
        self.counter = self.get_counter()  # Initialize the counter by fetching its last saved value
        # self.clear_database() # Uncomment to clear database

    def close(self):
        """ Updates counter and closes the database connection. """
        self.update_counter() 
        self.db.close()

    def clear_database(self):
        """ Clears all records in database. """
        cursor = self.db.cursor()
        try:
            record = cursor.first()
            while record:
                self.db.delete(record[0]) 
                record = cursor.next()
        finally:
            cursor.close()
        self.counter = 0  # Reset the counter if used for generating keys

    def get_counter(self):
        """ Retrieve the counter from the database using the '###counter' key.
        
        Returns:
        - int: The current value of the counter or 0 if it's not set.
        """
        try:
            counter_value = self.db.get(b'###counter')
            if counter_value is not None:
                return int(counter_value.decode())
            return 0  # Default to 0 if key does not exist
        except db.DBError as e:
            return 0

    def update_counter(self):
        """ Update the counter value in the database. """
        try:
            self.db.put(b'###counter', str(self.counter).encode())  
        except db.DBError as e:
            return

    def key_exists(self, key):
        """ Check if a given key exists in the database.  """
        try:
            value = self.db.get(key.encode()) 
            return value is not None
        except db.DBError as e:
            return False
            
    def drop_table(self, table_name):
        """ Deletes all records and schema associated with the specified table. """
        
        # Delete records in the table
        cursor = self.db.cursor()
        record = cursor.first()
        while record:
            key_prefix = table_name + "#"
            if record[0].decode().startswith(key_prefix):  #FIX naming convention
                self.db.delete(record[0])
            record = cursor.next()
        cursor.close()

        # Delete table schema
        self.db.delete(f"##{table_name}".encode())

    def delete_all_table_records(self, table_name):
        """
        Clears all records from the specified table.
        """
        cursor = self.db.cursor()
        record = cursor.first()
        while record:
            key, value = record
            if key.decode().startswith(f"{table_name}#"):
                self.db.delete(key)
            record = cursor.next()
        cursor.close()

    def delete_record(self, table_name, record):
        """
        Deletes a specified record from the database.
        """
        # Assuming each record can be uniquely identified by an 'id' key
        # key_to_delete = f"{table_name}#{record['#']}"
        key_to_delete = record['#']
        self.db.delete(key_to_delete.encode())

    def insert_table(self, table_name, schema):
        """
        Inserts a new table schema into the database.

        Parameters:
        - table_name (str): The name of the table.
        - schema (str): The encoded string for schema of the table.
        """
        schema_key = SCHEMA_KEY_PREFIX + table_name
        self.db.put(schema_key.encode(), schema.encode())

    def insert_row(self, table_name, row_values):
        """ 
        Insert a new row into the specified table. 

        Parameters:
        - table_name (str): The name of the table where the row will be inserted.
        - row_values (dict): A dictionary representing the values of the row to insert, which will be serialized into JSON.
        """
        key = self.generate_unique_key(table_name)
        
        row_values["#"] = key # Store key for record (needed for picking out records in DELETE and SELECT)
        
        serialized_value = json.dumps(row_values).encode('utf-8')
        
        try:
            self.db.put(key.encode(), serialized_value) 
            self.counter += 1 
            self.update_counter() 
        except db.DBError as e:
            return
    
    def generate_unique_key(self, table_name):
        """ Generate a unique key for a new record. """
        unique_key = f"{table_name}#{self.counter}"
        return unique_key
    
    def get_tables(self):
        """ Retrieves a list of all tables in the database. """
        tables = []
        cursor = self.db.cursor()
        record = cursor.first()
        while record:
            key = record[0].decode()
            if key.startswith(SCHEMA_KEY_PREFIX) and key != COUNTER_KEY:
                tables.append(key.lstrip(SCHEMA_KEY_PREFIX))
            record = cursor.next()
        return tables
    
    def get_table_schema(self, table_name):
        """
        Fetches the schema for the specified table.

        Parameters:
        - table_name (str): The name of the table whose schema is to be retrieved.

        Returns:
        - str or None: The schema of the table if available, or None if the table does not exist.
        """
        try:
            schema_bytes = self.db.get(table_name.encode())

            if schema_bytes:
                schema_str = schema_bytes.decode()
                return schema_str
            else:
                return None
        except db.DBError as e: 
            return None
        
    def retrieve_records(self, table_name):
        """
        Retrieves all records from the specified table, deserializing them from JSON.

        Parameters:
        - table_name (str): The name of the table from which to retrieve records.

        Returns:
        - list of dicts: A list of dictionaries representing each record in the table.
        """
        try:
            records = []
            cursor = self.db.cursor()
            record = cursor.first()
            while record:
                key, value = record
                if key.decode().startswith(table_name + "#"):  #FIX naming convention
                    record = json.loads(value.decode('utf-8'))
                    # print(record) #DELETE
                    records.append(record)  # Decoded JSON 
                record = cursor.next()
            cursor.close()
            return records
        except db.DBError as e: 
            return []
class CustomException(Exception):
    def __init__(self, message=""):
        self.message = message
        super().__init__(self.message)

class Message:
    # Messages without variables
    SYNTAX_ERROR = "Syntax Error"
    NO_SUCH_TABLE = "No such table"
    INSERT_RESULT = "1 row inserted"
    INSERT_TYPE_MISMATCH_ERROR = "Insertion has failed: Types are not matched"
    WHERE_INCOMPARABLE_ERROR = "Where clause trying to compare incomparable values"
    WHERE_TABLE_NOT_SPECIFIED = "Where clause trying to reference tables which are not specified"
    WHERE_COLUMN_NOT_EXIST = "Where clause trying to reference non existing column"
    WHERE_AMBIGUOUS_REFERENCE = "Where clause contains ambiguous reference"
    INSERT_DUPLICATE_PRIMARY_KEY_ERROR = "Insertion has failed: Primary key duplication"
    INSERT_REFERENTIAL_INTEGRITY_ERROR = "Insertion has failed: Referential integrity violation"
    DUPLICATE_COLUMN_DEF_ERROR = "Create table has failed: column definition is duplicated"
    DUPLICATE_PRIMARY_KEY_DEF_ERROR = "Create table has failed: primary key definition is duplicated"
    REFERENCE_TYPE_ERROR = "Create table has failed: foreign key references wrong type"
    REFERENCE_NON_PRIMARY_KEY_ERROR = "Create table has failed: foreign key references non primary key column"
    REFERENCE_COLUMN_EXISTENCE_ERROR = "Create table has failed: foreign key references non existing column"
    REFERENCE_TABLE_EXISTENCE_ERROR = "Create table has failed: foreign key references non existing table"
    TABLE_EXISTENCE_ERROR = "Create table has failed: table with the same name already exists"
    CHAR_LENGTH_ERROR = "Char length should be over 0"

    # Messages accepting variables
    CREATE_TABLE_SUCCESS = "'{}' table is created"
    INSERT_COLUMN_EXISTENCE_ERROR = "Insertion has failed: '{}' does not exist"
    INSERT_COLUMN_NON_NULLABLE_ERROR = "Insertion has failed: '{}' is not nullable"
    DELETE_RESULT = "'{}' row(s) deleted"
    SELECT_TABLE_EXISTENCE_ERROR = "Selection has failed: '{}' does not exist"
    SELECT_COLUMN_RESOLVE_ERROR = "Selection has failed: fail to resolve '{}'"
    DELETE_REFERENTIAL_INTEGRITY_PASSED = "'{}' row(s) are not deleted due to referential integrity"
    NON_EXISTING_COLUMN_DEF_ERROR = "Create table has failed: '{}' does not exist in column definition"
    DROP_SUCCESS = "'{}' table is dropped"
    DROP_REFERENCED_TABLE_ERROR = "Drop table has failed: '{}' is referenced by other table"

    # Project 1-2 Custom Exceptions
    REFERENCE_COLUMN_COUNT_MISMATCH_ERROR = "Create table has failed: number of referencing columns does not match number of referenced columns"
    REFERENCE_TABLE_SELF_ERROR = "Create table has failed: foreign key cannot reference its own table"
    INSERT_TABLE_DUPLICATE_COLUMN_ERROR = "Insert has failed: column name is duplicated"

    @staticmethod
    def get_message(message, name=None, count=None):
        if count is not None:
            return message.format(count)
        elif name is not None:
            return message.format(name)
        else:
            return message
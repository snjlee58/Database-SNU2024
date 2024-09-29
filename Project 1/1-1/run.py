from lark import Lark, Transformer, exceptions

# Input prompt
PROMPT = "DB_2020-16634> "

# Declaring Transformer class and transform methods
class MyTransformer(Transformer):
    def create_table_query(self, items):
        print(PROMPT + "\'CREATE TABLE\' requested")
    def drop_table_query(self, items):
        print(PROMPT + "\'DROP TABLE\' requested")
    def explain_query(self, items):
        print(PROMPT + "\'EXPLAIN\' requested")
    def describe_query(self, items):
        print(PROMPT + "\'DESCRIBE\' requested")
    def desc_query(self, items):
        print(PROMPT + "\'DESC\' requested")
    def select_query(self, items):
        print(PROMPT + "\'SELECT\' requested")
    def show_tables_query(self, items):
        print(PROMPT + "\'SHOW TABLES\' requested")
    def delete_query(self, items):
        print(PROMPT + "\'DELETE\' requested")
    def insert_query(self, items):
        print(PROMPT + "\'INSERT\' requested")
    def update_query(self, items):
        print(PROMPT + "\'UPDATE\' requested")
    def EXIT(self, items):
        exit()

# Open and read grammar from grammar.lark 
with open('grammar.lark') as file:
    sql_parser = Lark(file.read(), start="command", lexer="basic")

# Function to parse each individual query
def parse_query(query):
    try:
        output = sql_parser.parse(query)
        MyTransformer().transform(output)
        return True # Parsing was successful
    except exceptions.UnexpectedInput:
        print(PROMPT + "Syntax error")
        return False # Parsing failed

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
        # debug(user_input) 

        # Iterate through each individual query and process it through the lark parser
        for q in queries[:-1]:
            success = parse_query(q + ";")  # Add ";" back for parsing, deleted from split function
            if not success:
                break # Stop processing queries after a syntax error

if __name__ == "__main__":
	main()
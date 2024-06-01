from mysql.connector import connect, Error
import pandas as pd

connection = connect(
    host='astronaut.snu.ac.kr',
    port=7001,
    user='DB2020_16634',
    password='DB2020_16634',
    db='DB2020_16634',
    charset='utf8'
)

def initialize_database():
    try:
        # Read the CSV file
        data = pd.read_csv('data.csv', sep=',', encoding='latin1')

        with connection.cursor(dictionary=True) as cursor:

            # Create tables
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS books (
                b_id INT PRIMARY KEY,
                b_title VARCHAR(255) NOT NULL,
                b_author VARCHAR(255) NOT NULL
            );''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                u_id INT PRIMARY KEY,
                u_name VARCHAR(255) NOT NULL
            );''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS ratings (
                b_id INT NOT NULL,
                u_id INT NOT NULL,
                b_u_rating INT NOT NULL,
                FOREIGN KEY (b_id) REFERENCES books (b_id),
                FOREIGN KEY (u_id) REFERENCES users (u_id)
            );''')

            # Insert unique books
            books = data[['b_id', 'b_title', 'b_author']].drop_duplicates()
            for _, row in books.iterrows():
                sql = 'INSERT INTO books (b_id, b_title, b_author) VALUES (%s, %s, %s)'
                cursor.execute(sql, (row['b_id'], row['b_title'], row['b_author']))

            # Insert unique users
            users = data[['u_id', 'u_name']].drop_duplicates()
            for _, row in users.iterrows():
                sql = 'INSERT INTO users (u_id, u_name) VALUES (%s, %s);'
                cursor.execute(sql, (row['u_id'], row['u_name']))

            # Insert ratings
            ratings = data[['b_id', 'u_id', 'b_u_rating']]
            for _, row in ratings.iterrows():
                sql = 'INSERT INTO ratings (b_id, u_id, b_u_rating) VALUES (%s, %s, %s);'
                cursor.execute(sql, (row['b_id'], row['u_id'], row['b_u_rating']))

        # Commit changes
        connection.commit()

        print('Database successfully initialized')
    
    except Error as e:
        print(f"Error: {e}")
    
    finally:
        cursor.close()
        # connection.close()

def reset():
    # YOUR CODE GOES HERE
    with connection.cursor(dictionary=True) as cursor:
        sql = 'DROP TABLE books, users, ratings;'
        cursor.execute(sql)
        initialize_database()
        connection.commit()
    pass

def fetch(sql):
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
        return result

def print_books():
    print_books_sql = 'SELECT b_id, b_title, b_author FROM books;'
    books = fetch(print_books_sql)
    
    for book in books:
        b_id = book['b_id']
        b_title = book['b_title']
        b_author = book['b_author']
        book['avg_rating'] = 5
        book['available_copies'] = 1
    
    print(format_results('books', books))

def print_users():
    # YOUR CODE GOES HERE
    print_users_sql = 'SELECT u_id, u_name FROM users ORDER BY u_id;'
    users = fetch(print_users_sql)

    print(format_results('users', users))
    # print msg

def insert_book():
    title = input('Book title: ')
    author = input('Book author: ')
    # YOUR CODE GOES HERE
    # print msg
    pass

def remove_book():
    book_id = input('Book ID: ')
    # YOUR CODE GOES HERE
    # print msg
    pass

def insert_user():
    name = input('User name: ')
    # YOUR CODE GOES HERE
    # print msg
    pass

def remove_user():
    user_id = input('User ID: ')
    # YOUR CODE GOES HERE
    # print msg
    pass

def checkout_book():
    book_id = input('Book ID: ')
    user_id = input('User ID: ')
    # YOUR CODE GOES HERE
    # print msg
    pass

def return_and_rate_book():
    book_id = input('book ID: ')
    user_id = input('User ID: ')
    rating = input('Ratings (1~5): ')
    # YOUR CODE GOES HERE
    # print msg
    pass

def print_users_for_book():
    user_id = input('User ID: ')
    # YOUR CODE GOES HERE
    # print msg
    pass

def print_borrowing_status_for_user():
    user_id = input('User ID: ')
    # YOUR CODE GOES HERE
    # print msg
    pass

def search_books():
    query = input('Query: ')
    # YOUR CODE GOES HERE
    # print msg

def recommend_popularity():
    # YOUR CODE GOES HERE
    user_id = input('User ID: ')
    # YOUR CODE GOES HERE
    # print msg
    pass

def recommend_item_based():
    user_id = input('User ID: ')
    # YOUR CODE GOES HERE
    # print msg
    pass

def format_results(type, results):
    line = '--------------------------------------------------------------------------------------------------------------------------\n'
    res = line

    # Set Headers and Spacing Format
    if type == 'books':
        headers = ['b_id', 'b_title', 'b_author', 'avg_rating', 'available_copies']
        formats = [8, 50, 30, 16, 16]
    elif type == 'users':
        headers = ['u_id', 'u_name']
        formats = [8, 30]

    # Add Headers
    for i in range(len(headers)):
        res += f'{headers[i]:<{formats[i]}}'
    res += '\n'
    res += line

    # Add Results
    for row in results:
        temp_result = ''
        for i in range(len(headers)):
            temp_result += f'{row[headers[i]]:<{formats[i]}}'
        res += temp_result
        res += '\n'

    # No Result
    if not results:
        res += '\n'
    
    res += line
    
    return res

def main():
    while True:
        print('============================================================')
        print('1. initialize database')
        print('2. print all books')
        print('3. print all users')
        print('4. insert a new book')
        print('5. remove a book')
        print('6. insert a new user')
        print('7. remove a user')
        print('8. check out a book')
        print('9. return and rate a book')
        print('10. print borrowing status of a user')
        print('11. search books')
        print('12. recommend a book for a user using popularity-based method')
        print('13. recommend a book for a user using user-based collaborative filtering')
        print('14. exit')
        print('15. reset database')
        print('============================================================')
        menu = int(input('Select your action: '))

        if menu == 1:
            initialize_database()
        elif menu == 2:
            print_books()
        elif menu == 3:
            print_users()
        elif menu == 4:
            insert_book()
        elif menu == 5:
            remove_book()
        elif menu == 6:
            insert_user()
        elif menu == 7:
            remove_user()
        elif menu == 8:
            checkout_book()
        elif menu == 9:
            return_and_rate_book()
        elif menu == 10:
            print_borrowing_status_for_user()
        elif menu == 11:
            search_books()
        elif menu == 12:
            recommend_popularity()
        elif menu == 13:
            recommend_item_based()
        elif menu == 14:
            print('Bye!')
            break
        elif menu == 15:
            reset()
        else:
            print('Invalid action')


if __name__ == "__main__":
    main()

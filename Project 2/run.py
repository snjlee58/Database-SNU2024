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
                b_id INT AUTO_INCREMENT PRIMARY KEY,
                b_title VARCHAR(50) NOT NULL,
                b_author VARCHAR(50) NOT NULL,
                b_available_copies INT DEFAULT 1,
                b_avg_rating FLOAT DEFAULT 0,
                UNIQUE (b_title, b_author) 
            );''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                u_id INT AUTO_INCREMENT PRIMARY KEY,
                u_name VARCHAR(10) NOT NULL
            );''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS ratings (
                b_id INT NOT NULL,
                u_id INT NOT NULL,
                b_u_rating INT NOT NULL,
                FOREIGN KEY (b_id) REFERENCES books (b_id),
                FOREIGN KEY (u_id) REFERENCES users (u_id)
            );''')

            # Create borrowings table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS borrowings (
                borrow_id INT AUTO_INCREMENT PRIMARY KEY,
                b_id INT NOT NULL,
                u_id INT NOT NULL,
                returned BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (b_id) REFERENCES books(b_id),
                FOREIGN KEY (u_id) REFERENCES users(u_id)
            );
            ''')

            # Insert unique books
            books = data[['b_id', 'b_title', 'b_author']].drop_duplicates()
            for _, row in books.iterrows():
                b_id = int(row['b_id'])
                b_title = row['b_title']
                b_author = row['b_author']

                sql = 'INSERT INTO books (b_id, b_title, b_author) VALUES (%s, %s, %s);'
                cursor.execute(sql, (b_id, b_title, b_author))

            # Insert unique users
            users = data[['u_id', 'u_name']].drop_duplicates()
            for _, row in users.iterrows():
                u_id = int(row['u_id'])
                u_name = row['u_name']

                sql = 'INSERT INTO users (u_id, u_name) VALUES (%s, %s);'
                cursor.execute(sql, (u_id, u_name))

            # Insert ratings
            ratings = data[['b_id', 'u_id', 'b_u_rating']]
            for _, row in ratings.iterrows():
                b_id = int(row['b_id'])
                u_id = int(row['u_id'])
                b_u_rating = int(row['b_u_rating'])

                sql = 'INSERT INTO ratings (b_id, u_id, b_u_rating) VALUES (%s, %s, %s);'
                cursor.execute(sql, (b_id, u_id, b_u_rating))

            # Calculate and update average ratings for each book
            cursor.execute('SELECT b_id FROM books;')
            b_ids = cursor.fetchall()
            for book in b_ids:
                b_id = book['b_id']
                cursor.execute('SELECT AVG(b_u_rating) as avg_rating FROM ratings WHERE b_id = %s', (b_id,))
                avg_rating = cursor.fetchone()['avg_rating']
                cursor.execute('UPDATE books SET b_avg_rating = %s WHERE b_id = %s', (avg_rating, b_id))

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
        sql = 'DROP TABLE books, users, ratings, borrowings;'
        cursor.execute(sql)
        initialize_database()
        connection.commit()
    pass

# SELECT query. Return results.
def fetch(sql):
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
        return result

# INSERT, DELETE query.
def execute(sql, params=None):
    with connection.cursor(dictionary=True) as cursor:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        connection.commit()

def print_books():
    print_books_sql = 'SELECT * FROM books ORDER BY b_id;' 
    books = fetch(print_books_sql)

    print(format_results('books', books))

def print_users():
    print_users_sql = 'SELECT u_id, u_name FROM users ORDER BY u_id;'
    users = fetch(print_users_sql)

    print(format_results('users', users))

def insert_book():
    title = input('Book title: ')
    author = input('Book author: ')
    
    # YOUR CODE GOES HERE
    if not (1 <= len(title) <= 50):
        print("Title length should range from 1 to 50 characters") #E1
        return
    if not (1 <= len(author) <= 30):
        print("Author length should range from 1 to 30 characters") #E2
        return
    
    with connection.cursor() as cursor:
        # Check if the combination of title and author already exists
        cursor.execute('SELECT * FROM books WHERE b_title = %s AND b_author = %s', (title, author))
        result = cursor.fetchone()
        if result:
            print(f"Book [({title}, {author})] already exists") #E3
            return

        # Insert new book
        sql = 'INSERT INTO books (b_title, b_author) VALUES (%s, %s)'
        execute(sql, (title, author))
        print("One book successfully inserted") #S3

# 6. 도서 삭제
def remove_book():
    b_id = input('Book ID: ')
    # YOUR CODE GOES HERE
    
    # Check if the book exists
    if not book_exists(b_id):
        print(f'Book {b_id} does not exist')  # E5
        return
    
    with connection.cursor(dictionary=True) as cursor:
        # Check if the book is currently borrowed
        cursor.execute('SELECT * FROM borrowings WHERE b_id = %s AND returned IS FALSE', (b_id,))
        borrowing = cursor.fetchone()
        if borrowing:
            print("Cannot delete a book that is currently borrowed")  #E6
            return

        # Delete related ratings
        cursor.execute('DELETE FROM ratings WHERE b_id = %s', (b_id,))

        # Delete related borrowings
        cursor.execute('DELETE FROM borrowings WHERE b_id = %s', (b_id,))

        # Delete the book
        cursor.execute('DELETE FROM books WHERE b_id = %s', (b_id,))

        connection.commit()
        print("One book successfully removed")  # S5

def book_exists(b_id):
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute('SELECT * FROM books WHERE b_id = %s', (b_id,))
        book = cursor.fetchone()
        return book is not None

def user_exists(u_id):
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute('SELECT * FROM users WHERE u_id = %s', (u_id,))
        user = cursor.fetchone()
        return user is not None
    
def insert_user():
    name = input('User name: ')
    # YOUR CODE GOES HERE
    if not (1 <= len(name) <= 10):
        print("Username length should range from 1 to 10 characters") #E4
        return
    
    # Insert new user
    sql = 'INSERT INTO users (u_name) VALUES (%s);'
    execute(sql, (name,))
    print("One user successfully inserted") #S2

# 7. 회원 삭제
def remove_user():
    u_id = input('User ID: ')
    # YOUR CODE GOES HERE

    with connection.cursor(dictionary=True) as cursor:

        # Check if the user exists
        if not user_exists(u_id):
            print(f"User {u_id} does not exist") #E7
            return

        # Check if the user has books currently borrowed
        borrowed_books = get_borrowed_books(u_id)
        if borrowed_books:
            print("Cannot delete a user with borrowed books")  #E8
            return
        
        # Get the books rated by the user
        cursor.execute('SELECT b_id FROM ratings WHERE u_id = %s', (u_id,))
        rated_books = cursor.fetchall()
        # print(rated_books)

        # Delete related ratings
        cursor.execute('DELETE FROM ratings WHERE u_id = %s', (u_id,))

        # Update book ratings
        for book in rated_books:
            update_book_ratings(book['b_id'])

        # Delete related borrowings
        cursor.execute('DELETE FROM borrowings WHERE u_id = %s', (u_id,))

        # Delete the user
        cursor.execute('DELETE FROM users WHERE u_id = %s', (u_id,))

        connection.commit()
        print("One user successfully removed")  #S4

def available_copies_exists(b_id):
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute('SELECT b_available_copies FROM books WHERE b_id = %s', (b_id,))
        b_available_copies = cursor.fetchone()['b_available_copies']
        return b_available_copies > 0

def get_borrowed_books(u_id):
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute('SELECT * FROM borrowings WHERE u_id = %s AND returned = FALSE', (u_id,))
        borrowed_books = cursor.fetchall()
        return borrowed_books

def update_book_ratings(b_id):
    with connection.cursor(dictionary=True) as cursor:
        # Calculate new average rating
        cursor.execute('SELECT AVG(b_u_rating) as avg_rating FROM ratings WHERE b_id = %s', (b_id,))
        new_avg_rating = cursor.fetchone()['avg_rating']
        if new_avg_rating is None:
                new_avg_rating = 0  # Set to 0 if no ratings exist
        # print(new_avg_rating) #DELETE
        # Update the average rating in the books table
        cursor.execute('UPDATE books SET b_avg_rating = %s WHERE b_id = %s', (new_avg_rating, b_id))

# 8. 도서 대출
def checkout_book():
    b_id = input('Book ID: ')
    u_id = input('User ID: ')
    
    # YOUR CODE GOES HERE
    with connection.cursor(dictionary=True) as cursor:
        # Check if the book exists
        if not book_exists(b_id):
            print(f'Book {b_id} does not exist') #E5
            return

        # Check if the user exists
        if not user_exists(u_id):
            print(f"User {u_id} does not exist") #E7
            return

        # Check available copies
        if not available_copies_exists(b_id):
            print("Cannot check out a book that is currently borrowed") #E9
            return

        # Check user's borrowing count
        cursor.execute('SELECT COUNT(*) as borrow_count FROM borrowings WHERE u_id = %s AND returned = FALSE', (u_id,))
        borrow_count = cursor.fetchone()['borrow_count']
        if borrow_count >= 2:
            print(f"User {u_id} exceeded the maximum borrowing limit")  #E10
            return

        # Update available copies
        cursor.execute('UPDATE books SET b_available_copies = b_available_copies - 1 WHERE b_id = %s', (b_id,))

        # Insert borrowing record
        cursor.execute('INSERT INTO borrowings (b_id, u_id) VALUES (%s, %s)', (b_id, u_id))

        connection.commit()
        print("Book successfully checked out")  #S6

# 9. 도서 반납과 평점 부여
def return_and_rate_book():
    b_id = input('book ID: ')
    u_id = input('User ID: ')
    rating = input('Ratings (1~5): ')
    # YOUR CODE GOES HERE

    # Validate rating
    try:
        rating = int(rating)
        if not (1 <= int(rating) <= 5):
            raise Error
        
        with connection.cursor(dictionary=True) as cursor:
            # Check if the book exists
            if not book_exists(b_id):
                print(f'Book {b_id} does not exist') #E5
                return

            # Check if the user exists
            if not user_exists(u_id):
                print(f"User {u_id} does not exist") #E7
                return

            # Check if the book is currently borrowed by the user
            cursor.execute('SELECT * FROM borrowings WHERE b_id = %s AND u_id = %s AND returned = FALSE', (b_id, u_id))
            borrowing = cursor.fetchone()
            if not borrowing:
                print("Cannot return and rate a book that is not currently borrowed for this user")  #E12
                return

            # Update the borrowing record to mark the book as returned
            cursor.execute('UPDATE borrowings SET returned = TRUE WHERE b_id = %s AND u_id = %s', (b_id, u_id))

            # Update available copies
            cursor.execute('UPDATE books SET b_available_copies = b_available_copies + 1 WHERE b_id = %s', (b_id,))

            # Check if the user has already rated the book
            cursor.execute('SELECT * FROM ratings WHERE b_id = %s AND u_id = %s', (b_id, u_id))
            existing_rating = cursor.fetchone()
            if existing_rating:
                # Update existing rating
                cursor.execute('UPDATE ratings SET b_u_rating = %s WHERE b_id = %s AND u_id = %s', (rating, b_id, u_id))
            else:
                # Insert new rating
                cursor.execute('INSERT INTO ratings (b_id, u_id, b_u_rating) VALUES (%s, %s, %s)', (b_id, u_id, rating))

            # Update book ratings
            update_book_ratings(b_id)

            connection.commit()
            print("Book successfully returned and rated")  #S7
            return

    except:
        print("Rating should range from 1 to 5.")  #E11
        return
    
def print_users_for_book():
    u_id = input('User ID: ')
    # YOUR CODE GOES HERE
    # print msg
    pass

# 10. 회원이 대출 중인 도서 정보 출력
def print_borrowing_status_for_user():
    u_id = input('User ID: ')
    # YOUR CODE GOES HERE

    # Check if the user exists
    if not user_exists(u_id):
        print(f"User {u_id} does not exist") #E7
        return

    # Fetch borrowed books
    borrowed_books = fetch_borrowed_books(u_id)
    if not borrowed_books:
        print("No books currently borrowed by this user.") #FIX: check specs for this
        return

    # Print borrowed books information
    #FIX: incorporate into print formmated function if i can
    line = '-' * 100
    print(line)
    print(f'{"Book ID":<10} {"Title":<40} {"Author":<30} {"Average Rating":<15}')
    print(line)
    for book in borrowed_books:
        print(f'{book["b_id"]:<10} {book["b_title"]:<40} {book["b_author"]:<30} {book["b_avg_rating"]:<15}')
    print(line)
    # print msg
    # pass

def fetch_borrowed_books(u_id):
    # try:
        with connection.cursor(dictionary=True) as cursor:
            # Fetch borrowed books information
            cursor.execute('''
                SELECT b.b_id, b.b_title, b.b_author, b.b_avg_rating
                FROM books b
                JOIN borrowings br ON b.b_id = br.b_id
                WHERE br.u_id = %s AND br.returned = FALSE
                ORDER BY b.b_id ASC;
            ''', (u_id,))
            borrowed_books = cursor.fetchall()
            return borrowed_books
    # except Error as e:
    #     print(f"Error fetching borrowed books: {e}")
    #     return None

# 11. 도서 검색
def search_books():
    query = input('Query: ')
    # YOUR CODE GOES HERE
    # print msg

    with connection.cursor(dictionary=True) as cursor:
        # Perform a case-insensitive search
        cursor.execute('''
            SELECT b_id, b_title, b_author, b_avg_rating, b_available_copies
            FROM books
            WHERE LOWER(b_title) LIKE LOWER(%s)
            ORDER BY b_id ASC;
            ''', (f'%{query}%',))
        search_results = cursor.fetchall()

        # Print search results
        print(format_results('books', search_results)) #FIX: what if search results is empty?

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
    # Set Headers and Spacing Format
    if type == 'books':
        headers = ['b_id', 'b_title', 'b_author', 'b_avg_rating', 'b_available_copies']
        formats = [8, 50, 30, 16, 16]
    elif type == 'users':
        headers = ['u_id', 'u_name']
        formats = [8, 30]

    # Calculate length of separator
    total_length = sum(formats) + len(formats) - 1  # Adding len(formats) - 1 for spaces between columns
    line = '-' * total_length + '\n'
    # line = '--------------------------------------------------------------------------------------------------------------------------\n'
    res = line

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

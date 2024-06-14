from mysql.connector import connect, Error
import pandas as pd
import numpy as np
import warnings

# Suppress FutureWarning
warnings.filterwarnings("ignore", category=FutureWarning)

connection = connect(
    host='astronaut.snu.ac.kr',
    port=7001,
    user='DB2020_16634',
    password='DB2020_16634',
    db='DB2020_16634',
    charset='utf8'
)

# 1. 데이터베이스 초기화
def initialize_database():
    try:
        # Read the CSV file
        data = pd.read_csv('data.csv', sep=',', encoding='latin1') 

        with connection.cursor(dictionary=True) as cursor:
            # Drop all tables if they exist to clear the database
            cursor.execute('DROP TABLE IF EXISTS borrowings;')
            cursor.execute('DROP TABLE IF EXISTS ratings;')
            cursor.execute('DROP TABLE IF EXISTS users;')
            cursor.execute('DROP TABLE IF EXISTS books;')

            # Create tables (books, users, ratings, borrowings)
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS books (
                b_id INT AUTO_INCREMENT PRIMARY KEY,
                b_title VARCHAR(50) NOT NULL,
                b_author VARCHAR(30) NOT NULL,
                b_available_copies INT DEFAULT 1,
                b_avg_rating FLOAT,
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
                FOREIGN KEY (b_id) REFERENCES books (b_id) ON DELETE CASCADE, 
                FOREIGN KEY (u_id) REFERENCES users (u_id) ON DELETE CASCADE
            );''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS borrowings (
                borrow_id INT AUTO_INCREMENT PRIMARY KEY,
                b_id INT NOT NULL,
                u_id INT NOT NULL,
                returned BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (b_id) REFERENCES books(b_id) ON DELETE CASCADE,
                FOREIGN KEY (u_id) REFERENCES users(u_id) ON DELETE CASCADE
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

                update_latest_rating_for_user(b_id, u_id, b_u_rating)

            # Calculate and update average ratings for each book
            cursor.execute('SELECT b_id FROM books;')
            b_ids = cursor.fetchall()
            for book in b_ids:
                b_id = book['b_id']
                update_book_avg_ratings(b_id)

        # Commit changes
        connection.commit()
        print('Database successfully initialized')
    
    except Error as e:
        return
    
    finally:
        cursor.close()

# 15. 데이터베이스 리셋 및 생성
def reset():
    confirmation = input("Are you sure you want to reset the database? This will delete all existing tables and data. (y/n): ")
    if confirmation.lower() != 'y':
        return
    
    with connection.cursor(dictionary=True) as cursor:
        sql = 'DROP TABLE books, users, ratings, borrowings;'
        cursor.execute(sql)
        initialize_database()
        connection.commit()

# Clear Database
def clear_database():
    confirmation = input("Are you sure you want to clear the database? This will delete all existing tables and data. (y/n): ")
    if confirmation.lower() != 'y':
        return
    
    with connection.cursor(dictionary=True) as cursor:
        sql = 'DROP TABLE books, users, ratings, borrowings;'
        cursor.execute(sql)
        connection.commit()

# Helper function for executing SELECT queries. -> Returns fetched records.
def fetch(sql):
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
        return result

# Helper function for executing INSERT, DELETE queries.
def execute(sql, params=None):
    with connection.cursor(dictionary=True) as cursor:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        connection.commit()

# 2. 모든 도서 정보 출력
def print_books():
    print_books_sql = 'SELECT * FROM books ORDER BY b_id;' 
    books = fetch(print_books_sql)

    print(format_results('books', books))

# 3. 모든 회원 정보 출력
def print_users():
    print_users_sql = 'SELECT u_id, u_name FROM users ORDER BY u_id;'
    users = fetch(print_users_sql)

    print(format_results('users', users))

# 4. 도서 추가
def insert_book():
    title = input('Book title: ')
    author = input('Book author: ')
    
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
            print(f"Book ({title}, {author}) already exists") #E3
            return

        # Insert new book
        sql = 'INSERT INTO books (b_title, b_author) VALUES (%s, %s)'
        execute(sql, (title, author))
        print("One book successfully inserted") #S3

# 6. 도서 삭제
def remove_book():
    b_id = input('Book ID: ')
    
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
        print("One book successfully removed") #S5

# Helper function - checks whether book exists
def book_exists(b_id):
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute('SELECT * FROM books WHERE b_id = %s', (b_id,))
        book = cursor.fetchone()
        return book is not None

# Helper function - checks whether user exists
def user_exists(u_id):
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute('SELECT * FROM users WHERE u_id = %s', (u_id,))
        user = cursor.fetchone()
        return user is not None

# 5. 회원 등록
def insert_user():
    name = input('User name: ')

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

    with connection.cursor(dictionary=True) as cursor:

        # Check if the user exists
        if not user_exists(u_id):
            print(f"User {u_id} does not exist") #E7
            return

        # Check if the user has books currently borrowed
        borrowed_books = retrieve_user_borrowings(u_id)
        if borrowed_books:
            print("Cannot delete a user with borrowed books")  #E8
            return
        
        # Get the books rated by the user
        cursor.execute('SELECT b_id FROM ratings WHERE u_id = %s', (u_id,))
        rated_books = cursor.fetchall()

        # Delete related ratings
        cursor.execute('DELETE FROM ratings WHERE u_id = %s', (u_id,))

        # Update book ratings
        for book in rated_books:
            update_book_avg_ratings(book['b_id'])

        # Delete related borrowings
        cursor.execute('DELETE FROM borrowings WHERE u_id = %s', (u_id,))

        # Delete the user
        cursor.execute('DELETE FROM users WHERE u_id = %s', (u_id,))

        connection.commit()
        print("One user successfully removed") #S4

# Helper function - checks whether a book is available for checkout
def available_copies_exists(b_id):
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute('SELECT b_available_copies FROM books WHERE b_id = %s', (b_id,))
        b_available_copies = cursor.fetchone()['b_available_copies']
        return b_available_copies > 0

# Helper function - retrieve current borrowings by a user
def retrieve_user_borrowings(u_id):
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute('SELECT * FROM borrowings WHERE u_id = %s AND returned = FALSE', (u_id,))
        borrowed_books = cursor.fetchall()
        return borrowed_books

# Helper function - recalculate and updates average ratings for a book
def update_book_avg_ratings(b_id):
    with connection.cursor(dictionary=True) as cursor:
        # Calculate new average rating
        cursor.execute('SELECT ROUND(AVG(b_u_rating), 2) as avg_rating FROM ratings WHERE b_id = %s', (b_id,))
        new_avg_rating = cursor.fetchone()['avg_rating']
        if new_avg_rating is None:
            cursor.execute('UPDATE books SET b_avg_rating = NULL WHERE b_id = %s', (b_id,)) # Set to NULL if no ratings exist
            return

        # Update the average rating in the books table
        cursor.execute('UPDATE books SET b_avg_rating = %s WHERE b_id = %s', (new_avg_rating, b_id))

# 8. 도서 대출
def checkout_book():
    b_id = input('Book ID: ')
    u_id = input('User ID: ')
    
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
    b_id = input('Book ID: ')
    u_id = input('User ID: ')
    rating = input('Ratings (1~5): ')

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

            # Update user's latest rating on the book
            update_latest_rating_for_user(b_id, u_id, rating)

            # Update book ratings
            update_book_avg_ratings(b_id)

            connection.commit()
            print("Book successfully returned and rated")  #S7
            return

    except:
        print("Rating should range from 1 to 5.")  #E11
        return
    
# Helper function - replaces a user's preexisting rating for a book 
def update_latest_rating_for_user(b_id, u_id, rating):
    with connection.cursor(dictionary=True) as cursor:
        # Check if the user has already rated the book
        cursor.execute('SELECT * FROM ratings WHERE b_id = %s AND u_id = %s', (b_id, u_id))
        existing_rating = cursor.fetchone()
        
        if existing_rating:
            # Update existing rating
            cursor.execute('UPDATE ratings SET b_u_rating = %s WHERE b_id = %s AND u_id = %s', (rating, b_id, u_id))
        else:
            # Insert new rating
            cursor.execute('INSERT INTO ratings (b_id, u_id, b_u_rating) VALUES (%s, %s, %s)', (b_id, u_id, rating))

# 10. 회원이 대출 중인 도서 정보 출력
def print_borrowing_status_for_user():
    u_id = input('User ID: ')

    # Check if the user exists
    if not user_exists(u_id):
        print(f"User {u_id} does not exist") #E7
        return

    # Fetch borrowed books
    borrowed_books = fetch_borrowed_books(u_id)

    # Print borrowed books information
    print(format_results('borrowings', borrowed_books))

# Helper function - retrieves borrowed book information by user
def fetch_borrowed_books(u_id):
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

# 11. 도서 검색
def search_books():
    query = input('Query: ')

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
        print(format_results('books', search_results)) 

# 12. 회원을 위한 도서 추천 1
def recommend_popularity():
    u_id = input('User ID: ')

    # Check if the user exists
    if not user_exists(u_id):
        print(f"User {u_id} does not exist") #E7
        return

    with connection.cursor(dictionary=True) as cursor:
        # Fetch books not rated by the user
        cursor.execute('''
            SELECT b.b_id, b.b_title, b.b_author, b.b_avg_rating, b.b_available_copies, COUNT(r.b_u_rating) as rating_count
            FROM books b
            LEFT JOIN ratings r ON b.b_id = r.b_id
            WHERE b.b_id NOT IN (SELECT b_id FROM ratings WHERE u_id = %s)
            GROUP BY b.b_id
            ORDER BY b.b_avg_rating DESC, b.b_id ASC;
        ''', (u_id,))
        recommendations = cursor.fetchall()

        if not recommendations:
            rating_based_book = None
            popularity_based_book = None
        else:
            # Display the top recommendations
            rating_based_book = recommendations[0]
            popularity_based_book = sorted(recommendations, key=lambda x: (-x['rating_count'], x['b_id']))[0]

        format_book_recommendations("Rating-based", rating_based_book)
        format_book_recommendations("Popularity-based", popularity_based_book)
        line = '-' * 107
        print(line)

# 13. 회원을 위한 도서 추천 2
def recommend_item_based():
    u_id = input('User ID: ')

    # Check if the user exists
    if not user_exists(u_id):
        print(f"User {u_id} does not exist") #E7
        return
    
    # Fetch all users and ratings data
    users_df = get_all_users()
    ratings_df = get_ratings_data()

    # Build user-item matrix
    user_item_matrix, original_ratings_mask = build_user_item_matrix(users_df, ratings_df)

    # Calculate cosine similarity matrix
    similarity_matrix = calculate_cosine_similarity(user_item_matrix)

    # Predict ratings for books not rated by the user
    predicted_ratings = predict_ratings(user_item_matrix, original_ratings_mask, similarity_matrix, u_id)
        
    if not predicted_ratings: 
        print(format_results('books_recommendation_cf', []))
        return

    # Sort the predicted ratings by rating (descending) and b_id (ascending)
    sorted_recommendations = sorted(predicted_ratings.items(), key=lambda x: (-x[1], x[0]))
    recommended_book_id, predicted_rating = sorted_recommendations[0]

    with connection.cursor(dictionary=True) as cursor:
        cursor.execute('SELECT b_id, b_title, b_author, b_avg_rating, b_available_copies FROM books WHERE b_id = %s', (recommended_book_id,))
        recommended_book = cursor.fetchone()
        recommended_book['predicted_rating'] = round(predicted_rating, 2)
        print(format_results('books_recommendation_cf', [recommended_book]))

# Helper function - retrieve all user ids
def get_all_users():
    try:
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute('SELECT u_id FROM users')
            users = cursor.fetchall()
            return pd.DataFrame(users)
    except Error as e:
        return pd.DataFrame()

# Helper function - retrieve all rating information
def get_ratings_data():
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute('SELECT u_id, b_id, b_u_rating FROM ratings')
        ratings = cursor.fetchall()
        return pd.DataFrame(ratings)

# Helper function - build user-item matrix 
def build_user_item_matrix(users_df, ratings_df):
    # Drop empty entries in users_df
    users_df = users_df.dropna(subset=['u_id']) 

    # Construct user-item matrix with actual ratings
    user_item_matrix = ratings_df.pivot(index='u_id', columns='b_id', values='b_u_rating')

    user_item_matrix = users_df.set_index('u_id').join(user_item_matrix, how='left') # Join with user table to include users with no ratings

    # Track which ratings were originally missing (False: no rating)
    original_ratings_mask = user_item_matrix.notna()

    # Calculate the average rating for each user
    user_avg_ratings = user_item_matrix.mean(axis=1).fillna(0)

    # Fill missing ratings with the user's average rating
    user_item_matrix = user_item_matrix.apply(lambda row: row.fillna(user_avg_ratings[row.name]), axis=1)

    return user_item_matrix, original_ratings_mask

# Helper function - calculate cosine similarity
def calculate_cosine_similarity(user_item_matrix):
    similarity_matrix = np.dot(user_item_matrix, user_item_matrix.T)
    norms = np.array([np.sqrt(np.diagonal(similarity_matrix))])

     # Handle division by zero and invalid values by replacing them with 0
    with np.errstate(divide='ignore', invalid='ignore'):
        similarity_matrix = similarity_matrix / norms / norms.T
        similarity_matrix[np.isnan(similarity_matrix)] = 0

    # Ensure the diagonal values are set to 1 for self-similarity
    np.fill_diagonal(similarity_matrix, 1)

    return similarity_matrix

# Helper function - calculate weighted predicted scores
def predict_ratings(user_item_matrix, original_ratings_mask, similarity_matrix, target_user_id):
    target_user_id = int(target_user_id)  # Explicit conversion to integer

    user_index = user_item_matrix.index.get_loc(target_user_id)
    user_ratings = user_item_matrix.loc[target_user_id]
    not_rated_by_user = original_ratings_mask.loc[target_user_id][original_ratings_mask.loc[target_user_id] == False].index

    predicted_ratings = {}
    for item_id in not_rated_by_user:
        item_ratings = user_item_matrix[item_id]
        user_avg_rating = item_ratings[target_user_id]
        weighted_sum = np.dot(similarity_matrix[user_index], item_ratings) - user_avg_rating
        sum_of_similarities = np.sum(np.abs(similarity_matrix[user_index])) - 1
        predicted_ratings[item_id] = weighted_sum / sum_of_similarities if sum_of_similarities != 0 else 0

    return predicted_ratings

# Helper function - formatting results for print
def format_results(type, results): 
    # Set Headers and Spacing Format
    if type == 'books':
        headers = ['id', 'title', 'author', 'avg.rating', 'quantity']
        db_fields = ['b_id', 'b_title', 'b_author', 'b_avg_rating', 'b_available_copies']
        formats = [8, 51, 31, 16, 16]
    elif type == 'users':
        headers = ['id', 'name']
        db_fields = ['u_id', 'u_name']
        formats = [8, 31]
    elif type =='borrowings':
        headers = ['id', 'title', 'author', 'avg.rating']
        db_fields = ['b_id', 'b_title', 'b_author', 'b_avg_rating']
        formats = [8, 51, 31, 16]
    elif type == 'books_recommendation':
        headers = ['id', 'title', 'author', 'avg.rating']
        db_fields = ['b_id', 'b_title', 'b_author', 'b_avg_rating']
        formats = [8, 51, 31, 16]
    elif type == 'books_recommendation_cf':
        headers = ['id', 'title', 'author', 'avg.rating', 'exp.rating']
        db_fields = ['b_id', 'b_title', 'b_author', 'b_avg_rating', 'predicted_rating']
        formats = [8, 51, 31, 16, 16]

    # Calculate length of separator
    total_length = sum(formats) + len(formats) - 1  # Adding len(formats) - 1 for spaces between columns
    line = '-' * total_length + '\n'
    res = line

    # Add Headers
    for i in range(len(headers)):
        res += f'{headers[i]:<{formats[i]}}'
    res += '\n'
    res += line

    # Add Results
    for row in results:
        temp_result = ''
        for i in range(len(db_fields)):
            value = row[db_fields[i]]
            if db_fields[i] == 'b_avg_rating' and value is None: 
                value = 'None'
            temp_result += f'{value:<{formats[i]}}'
        res += temp_result
        res += '\n'
    
    res += '-' * total_length
    
    return res


def format_book_recommendations(title, book):
    # Set Headers and Spacing Format
    headers = ['id', 'title', 'author', 'avg.rating']
    db_fields = ['b_id', 'b_title', 'b_author', 'b_avg_rating']
    formats = [8, 50, 30, 16]

    # Calculate length of separator
    total_length = sum(formats) + len(formats) - 1  # Adding len(formats) - 1 for spaces between columns
    line = '-' * total_length
    
    header_line = ''.join(f'{headers[i]:<{formats[i]}}' for i in range(len(headers)))

    print(line)
    print(title)
    print(line)
    print(header_line)
    print(line)
    
    if book:
        book_line = ''.join(f'{str(book.get(key, "None") if book.get(key, "None") is not None else "None"):<{formats[i]}}' for i, key in enumerate(db_fields))
        print(book_line)

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
            connection.close()
            print('Bye!')
            break
        elif menu == 15:
            reset()
        else:
            print('Invalid action')


if __name__ == "__main__":
    main()

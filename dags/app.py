from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.utils.log.logging_mixin import LoggingMixin
import logging

headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    'fetch_and_store_amazon_books',
    default_args=default_args,
    description='A simple DAG to fetch book data from Amazon and store it in Postgres',
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    tags=['amazon', 'books', 'etl']
)
def amazon_books_etl():

    @task()
    def get_amazon_data_books(num_books: int = 50):
        logger = LoggingMixin().log
        try:
            base_url = f"https://www.amazon.com/s?k=data+engineering+books"
            books = []
            seen_titles = set()
            page = 1

            while len(books) < num_books:
                try:
                    url = f"{base_url}&page={page}"
                    response = requests.get(url, headers=headers, timeout=10)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.content, "html.parser")
                    book_containers = soup.find_all("div", {"class": "s-result-item"})
                    
                    if not book_containers:
                        logger.warning(f"No books found on page {page}")
                        break
                    
                    for book in book_containers:
                        title = book.find("span", {"class": "a-text-normal"})
                        author = book.find("a", {"class": "a-size-base"})
                        price = book.find("span", {"class": "a-price-whole"})
                        rating = book.find("span", {"class": "a-icon-alt"})
                        
                        if title and author and price and rating:
                            book_title = title.text.strip()
                            
                            if book_title not in seen_titles:
                                seen_titles.add(book_title)
                                books.append({
                                    "Title": book_title,
                                    "Author": author.text.strip(),
                                    "Price": price.text.strip(),
                                    "Rating": rating.text.strip(),
                                })
                    
                    page += 1

                except requests.RequestException as e:
                    logger.error(f"Error fetching page {page}: {str(e)}")
                    break

            if not books:
                raise ValueError("No books were successfully scraped")
                
            df = pd.DataFrame(books)
            if df.empty:
                raise ValueError("No books were found to process")
                
            df.drop_duplicates(subset="Title", inplace=True)
            if len(df) == 0:
                raise ValueError("No unique books remained after deduplication")
                
            logger.info(f"Successfully scraped {len(df)} unique books")
            return df.to_dict('records')
            
        except Exception as e:
            logger.error(f"Failed to scrape Amazon books: {str(e)}")
            raise

    @task()
    def clean_book_data(book_data: list) -> list:
        logger = LoggingMixin().log
        
        if not book_data:
            raise ValueError("Received empty book data")
            
        cleaned_books = []
        error_count = 0
        
        for book in book_data:
            try:
                # Clean price - remove currency symbol and convert to float
                price_str = book.get('Price', '').replace('$', '').strip()
                if not price_str:
                    raise ValueError("Price is missing")
                price = float(price_str)
                
                # Clean rating - extract number from string
                rating_str = book.get('Rating', '').split(' ')[0]
                if not rating_str:
                    raise ValueError("Rating is missing")
                rating = float(rating_str)
                
                if not book.get('Title'):
                    raise ValueError("Title is missing")
                if not book.get('Author'):
                    raise ValueError("Author is missing")
                    
                cleaned_books.append({
                    'Title': book['Title'][:255],
                    'Author': book['Author'][:255],
                    'Price': price,
                    'Rating': rating
                })
                
            except (ValueError, KeyError) as e:
                error_count += 1
                logger.warning(f"Invalid book record: {str(e)}, Data: {book}")
                continue
        
        if not cleaned_books:
            raise ValueError(f"No valid books after cleaning. {error_count} records were invalid.")
        
        logger.info(f"Cleaned {len(cleaned_books)} book records. Skipped {error_count} invalid records.")
        return cleaned_books

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='books_connection',
        sql="""
        CREATE TABLE IF NOT EXISTS books (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            authors VARCHAR(255),
            price NUMERIC(10,2),
            rating NUMERIC(3,1),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    @task()
    def insert_book_data_into_postgres(book_data):
        logger = LoggingMixin().log
        
        if not book_data:
            raise ValueError("No book data provided for database insertion")

        postgres_hook = PostgresHook(postgres_conn_id='books_connection')
        insert_query = """
        INSERT INTO books (title, authors, price, rating)
        VALUES (%s, %s, %s, %s)
        """
        
        inserted_count = 0
        error_count = 0
        
        for book in book_data:
            try:
                postgres_hook.run(insert_query, parameters=(
                    book['Title'], 
                    book['Author'], 
                    book['Price'], 
                    book['Rating']
                ))
                inserted_count += 1
            except Exception as e:
                error_count += 1
                logger.error(f"Failed to insert book: {str(e)}, Data: {book}")
                
        if inserted_count == 0:
            raise ValueError(f"Failed to insert any books. {error_count} insertion errors occurred.")
            
        logger.info(f"Successfully inserted {inserted_count} books. Failed to insert {error_count} books.")

    # Define the flow of tasks
    raw_book_data = get_amazon_data_books()
    cleaned_book_data = clean_book_data(raw_book_data)
    create_table >> insert_book_data_into_postgres(cleaned_book_data)

# Create the DAG
dag = amazon_books_etl()

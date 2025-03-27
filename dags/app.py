from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.utils.log.logging_mixin import LoggingMixin
import logging
import random
import time

# Rotating user agents to reduce chance of being blocked
USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
]

def get_headers():
    return {
        "Referer": 'https://www.amazon.com/',
        "Sec-Ch-Ua": "Not_A Brand",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "macOS",
        'User-agent': random.choice(USER_AGENTS)
    }

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),  # Changed to a past date
    'retries': 2,  # Increased retries
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
    def get_amazon_data_books(num_books: int = 50, max_pages: int = 10):
        logger = LoggingMixin().log
        try:
            base_url = f"https://www.amazon.com/s?k=data+engineering+books"
            books = []
            seen_titles = set()
            page = 1

            while len(books) < num_books and page <= max_pages:  # Added max_pages limit
                try:
                    url = f"{base_url}&page={page}"
                    
                    # Use rotating headers and add random delay to be more human-like
                    response = requests.get(url, headers=get_headers(), timeout=15)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.content, "html.parser")
                    book_containers = soup.find_all("div", {"class": "s-result-item"})
                    
                    if not book_containers:
                        logger.warning(f"No books found on page {page}")
                        break
                    
                    books_found_on_page = 0
                    
                    for book in book_containers:
                        # More robust selector patterns, looking for different possible classes
                        title_elem = book.select_one("h2 .a-text-normal, .a-text-normal")
                        author_elem = book.select_one(".a-size-base.a-link-normal, .a-row .a-size-base")
                        
                        # Look for both whole and fraction parts of price
                        price_whole = book.select_one(".a-price-whole")
                        price_fraction = book.select_one(".a-price-fraction")
                        
                        rating_elem = book.select_one(".a-icon-alt, .a-star-medium-4 .a-icon-alt")
                        
                        if title_elem:
                            book_title = title_elem.text.strip()
                            
                            if book_title and book_title not in seen_titles:
                                seen_titles.add(book_title)
                                
                                # Get price as a string with both whole and fraction parts
                                price_text = ""
                                if price_whole:
                                    price_text = price_whole.text.strip()
                                    if price_fraction:
                                        price_text += "." + price_fraction.text.strip()
                                
                                # Get author or "Unknown" if not found
                                author_text = author_elem.text.strip() if author_elem else "Unknown"
                                
                                # Get rating or "0" if not found
                                rating_text = "0"
                                if rating_elem:
                                    rating_parts = rating_elem.text.strip().split()
                                    if rating_parts and rating_parts[0].replace('.', '', 1).isdigit():
                                        rating_text = rating_parts[0]
                                
                                books.append({
                                    "Title": book_title,
                                    "Author": author_text,
                                    "Price": price_text or "0",
                                    "Rating": rating_text,
                                })
                                books_found_on_page += 1
                    
                    logger.info(f"Found {books_found_on_page} new books on page {page}")
                    page += 1
                    
                    # Add random delay between requests to avoid being blocked
                    time.sleep(random.uniform(2, 5))

                except requests.RequestException as e:
                    logger.error(f"Error fetching page {page}: {str(e)}")
                    time.sleep(10)  # Wait longer after an error
                    if page < max_pages:
                        page += 1  # Try next page even after error
                    else:
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
                price_str = book.get('Price', '0').replace('$', '').strip()
                if not price_str or price_str == '':
                    price_str = '0'
                price = float(price_str)
                
                # Clean rating - extract number from string and handle missing ratings
                rating_str = book.get('Rating', '0')
                if not rating_str or rating_str == '':
                    rating_str = '0'
                try:
                    rating = float(rating_str)
                    # Ensure rating is between 0 and 5
                    rating = min(max(rating, 0), 5)
                except ValueError:
                    # Default to 0 if can't convert
                    rating = 0.0
                
                # Get a title or skip record
                if not book.get('Title'):
                    raise ValueError("Title is missing")
                
                # Use "Unknown" for missing author
                author = book.get('Author', 'Unknown')
                if not author or author.strip() == '':
                    author = 'Unknown'
                    
                cleaned_books.append({
                    'Title': book['Title'][:255],
                    'Author': author[:255],
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
        
        # Define batch size for more efficient inserts
        BATCH_SIZE = 100
        inserted_count = 0
        error_count = 0
        
        try:
            # Create batches for better performance
            batches = [book_data[i:i + BATCH_SIZE] for i in range(0, len(book_data), BATCH_SIZE)]
            
            with postgres_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    for batch in batches:
                        # Create values for batch insert
                        values_list = []
                        for book in batch:
                            try:
                                # Append tuple of values for each book
                                values_list.append((
                                    book['Title'], 
                                    book['Author'], 
                                    book['Price'], 
                                    book['Rating']
                                ))
                            except Exception as e:
                                error_count += 1
                                logger.error(f"Failed to prepare book for insertion: {str(e)}, Data: {book}")
                        
                        if values_list:
                            # Using executemany for batch inserts
                            cur.executemany("""
                                INSERT INTO books (title, authors, price, rating)
                                VALUES (%s, %s, %s, %s)
                            """, values_list)
                            
                            inserted_count += len(values_list)
                            logger.info(f"Inserted batch of {len(values_list)} books")
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Database batch insertion error: {str(e)}")
            raise
                
        if inserted_count == 0:
            raise ValueError(f"Failed to insert any books. {error_count} insertion errors occurred.")
            
        logger.info(f"Successfully inserted {inserted_count} books. Failed to insert {error_count} books.")

    # Define the flow of tasks
    raw_book_data = get_amazon_data_books()
    cleaned_book_data = clean_book_data(raw_book_data)
    create_table >> insert_book_data_into_postgres(cleaned_book_data)

# Create the DAG
dag = amazon_books_etl()

import polars as pl
import psycopg2
from dotenv import load_dotenv


load_dotenv()

try:
    # Connect to the PostgreSQL database
    connection = psycopg2.connect(
        dbname=os.,
        user="your_username",
        password="your_password",
        host="your_host",
        port="your_port"
    )
    print("Connected to the database successfully!")
except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL:", error)
return None


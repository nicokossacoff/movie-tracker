import pandas as pd
import numpy as np
import sqlite3

class Database:
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn = None
        self.cursor = None
    
    def connect(self):
        try:
            # Connects to the database
            self.conn = sqlite3.connect(self.db_name)

            # Creates a cursor object to interact with the database execute SQL queries
            self.cursor = self.conn.cursor()
        except sqlite3.Error as e:
            print('An error occured: ', {e})
            self.conn = None
            self.cursor = None

    def disconnect(self):
        if self.conn:
            self.conn.close()

    def create_table(self):
        try:
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS Movies (
                    reference_id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    title                       TEXT NOT NULL,
                    director                    TEXT,
                    release_date                DATE,
                    genre                       TEXT,
                    rating                      REAL,
                    watched_date                DATE
                );
            ''')
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"An error occured while creating the table: {e}")

    def add_movie(self, title: str, director: str = None, release_date: str = None, genre: str = None, rating: str = None, watched_date: str = None):
        try:
            self.cursor.execute("""
                INSERT INTO Movies (title, director, release_date, genre, rating, watched_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (title, director, release_date, genre, rating, watched_date))
            self.conn.commit()
            print(f"Movie '{title}' added successfully.")
        except sqlite3.Error as e:
            print(f"Error adding movie: {e}")

    def get_movies(self):
        try:
            self.cursor.execute("SELECT * FROM Movies")
            movies = self.cursor.fetchall()
            return movies
        except sqlite3.Error as e:
            print(f"Error retrieving movies: {e}")
            return None

    def get_title(self, title: str = None):
        try:
            self.cursor.execute("SELECT * FROM Movies WHERE title = ?", (title,))
            movie = self.cursor.fetchone()
            return movie
        except sqlite3.Error as e:
            print(f"Error retrieving movie: {e}")
            return None

    def delete_movie(self, title: str = None):
         try:
            self.cursor.execute("DELETE FROM Movies WHERE title = ?", (title,))
            self.conn.commit()
            print(f"Movie '{title}' deleted successfully.")
         except sqlite3.Error as e:
            print(f"Error deleting movie: {e}")
import pandas as pd
import numpy as np
import sqlite3

class Database:
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn = None
        self.cursor = None
    
    def connect(self):
        '''
        Connects to the database.
        '''
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
        '''
        Disconnects from the database.
        '''
        if self.conn:
            self.conn.close()

    def create_table(self, query: str):
        '''
        Creates a table in the database.

        Parameters:
        :query: str | SQL query to create the table.
        '''
        try:
            print('Creating table...')

            # Checks if the query starts with 'CREATE TABLE IF NOT EXISTS'
            if not query.startswith('CREATE TABLE IF NOT EXISTS'):
                raise ValueError("Query must start with 'CREATE TABLE IF NOT EXISTS'")
            else:
                # Executes the query
                self.cursor.execute(query)

            # Commits the changes to the database
            self.conn.commit()

            print('Table created successfully.')
        except sqlite3.Error as e:
            print(f"An error occured while creating the table: {e}")

    def add_movie(self, table_name: str, title: str, director: str = None, release_date: str = None, genre: str = None, rating: str = None, watched_date: str = None):
        '''
        Adds a movie to the database.

        Parameters:
        :table_name: str | Name of the table to add the movie to.
        :title: str | Title of the movie.
        :director: str | Director of the movie.
        :release_date: str | Release date of the movie.
        :genre: str | Genre of the movie.
        :rating: str | Rating of the movie.
        :watched_date: str | Date the movie was watched.
        '''
        try:
            print(f'Adding {title} into the database...')

            # Execute the insertion query
            self.cursor.execute(f"""
                INSERT INTO {table_name} (title, director, release_date, genre, rating, watched_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (title, director, release_date, genre, rating, watched_date))

            # Commit the changes to the database
            self.conn.commit()

            print(f"Movie '{title}' added successfully.")
        except sqlite3.Error as e:
            print(f"Error adding {title} to the database. {e}")

    def get_movies(self):
        '''
        Retrieves all movies from the database.
        '''
        try:
            # Execute the query to retrieve all movies
            self.cursor.execute("SELECT * FROM Movies")

            # Fetch all the movies
            while True:
                movie = self.cursor.fetchone()
                if movie is None:
                    break

                yield movie
        except sqlite3.Error as e:
            print(f"Error retrieving movies: {e}")
            return None

    def get_title(self, title: str = None) -> str:
        '''
        Searches for a movie by title.

        Parameters:
        :title: str | Title of the movie to search for.

        Returns:
        :movie: str | Movie found in the database.
        '''
        try:
            # Execute the query to retrieve the movie
            self.cursor.execute("SELECT * FROM Movies WHERE title = ?", (title,))

            # Fetch the movie
            movie = self.cursor.fetchone()
            return movie
        except sqlite3.Error as e:
            print(f"Error retrieving movie: {e}")
            return None

    def delete_movie(self, title: str = None):
        '''
        Deletes a movie from the database. Beware, this action is irreversible.

        Parameters:
        :title: str | Title of the movie to delete.
        '''
        try:
            print(f"Deleting movie '{title}'...")

            # Execute the query to delete the movie
            self.cursor.execute("DELETE FROM Movies WHERE title = ?", (title,))

            # Commit the changes to the database
            self.conn.commit()

            print(f"Movie '{title}' deleted successfully.")
        except sqlite3.Error as e:
            print(f"Error deleting movie: {e}")
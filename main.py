import argparse
import csv
import datetime
import json
import logging
import os
import re
import sqlite3
import time

import psycopg2

# Database connection parameters
db_params = {
    "dbname": "northwind",
    "user": "northwind_user",
    "password": "thewindisblowing",
    "host": "localhost",
    "port": "5432"
}

# Csv path
csv_file_path = "code-challenge-main/data/order_details.csv"

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class DataSaver:
    def __init__(self):
        self.db = None
        self.csv = None
        self.db_data = {}
        self.csv_data = {}
        self.local_data = []
        self.current_working_date = ''

        self.steps = [
            {
                'step': '1',
                'name': 'Initialize Data Sources',
                'description': 'This sets up a connection with the Postgres DB and reads the CSV data.',
                'function': self.initialize_data_sources,
                'additional_steps': [
                    {
                        'step': '1.1',
                        'name': 'Extract data from Data Sources',
                        'description': 'This loads into memory the needed data from the DB and the CSV to be processed.',
                        'function': self.extract_data_from_sources
                    },
                    {
                        'step': '1.2',
                        'name': 'Write data to disk',
                        'description': 'This takes the data loaded to the memory, and writes to disk, '
                                       'creating a new folder for every table in the database.\n'
                                       '  It then saves the file inside a new folder, '
                                       'created with the current date with the filename retrieved from the table and filetype "json".',
                        'function': self.write_datas_to_disk
                    }
                ]
            },
            {
                'step': '2',
                'name': 'Load saved data from disk to memory',
                'description': 'This loads the saved data (paths) into memory to be processed.',
                'function': self.load_saved_data_to_memory,
                'additional_steps': [
                    {
                        'step': '2.1',
                        'name': 'Save the data from memory to the new database',
                        'description': 'This loops trough ALL data paths, creating tables and columns programatically.\n'
                                       '  Essentially, this merge the two data from CSV and database into one final database.',
                        'function': self.create_new_database
                    }
                ]
            },
            {
                'step': '3',
                'name': 'Query Orders and Orders details',
                'description': 'This querys the orders and details from the merged database.\n'
                               '  It then saves the query to /query_output/ with the formatting: query_YYYY-MM-DD_generated_at_YYYY-MM-DD_HH-MM-SS.json',
                'function': self.query_orders,
                'additional_steps': []
            }
        ]

    def initialize_data_sources(self):
        try:
            self.db = DbInput(self)
            self.csv = CsvInput(self)
            self.db_data = {}
            self.csv_data = {}
            self.local_data = []
            self.current_working_date = ''
            return True
        except Exception as e:
            logging.error(e)
            logging.warning("Maybe Docker container is offline?")
            logging.warning("Try starting it inside the code-challenge-data folder using: docker-compose up")

            return False

    def create_new_database(self, date=""):

        try:
            os.makedirs('./merged_databases', exist_ok=True)

            formatted_date = date
            if date == "":
                now = datetime.datetime.now()
                formatted_date = now.strftime("%Y-%m-%d")

            if os.path.exists(f'./merged_databases/merged_database_date-{formatted_date}.db'):
                os.remove(f'./merged_databases/merged_database_date-{formatted_date}.db')

                logging.warning(
                    f"Database merged_database_date-{formatted_date}.db removed as it is being regenerated.\n")

            db_connection = sqlite3.connect(f'./merged_databases/merged_database_date-{formatted_date}.db')

            for path in self.local_data:
                self.create_table_from_data(path, db_connection)

            db_connection.close()

            logging.info(f"Created database with name: merged_database_date-{formatted_date}.db\n")
            return True
        except Exception as e:
            logging.error(e)
            return False

    def create_table_from_data(self, dir, connection):
        # Connect to the SQLite database
        db_cursor = connection.cursor()
        with open(dir, 'r') as file:
            json_data = json.load(file)

            column_data_types = {'int': 'INTEGER', 'str': 'TEXT', 'float': 'REAL'}  # Map data types
            table_name = dir.split('\\')[-1].split('.')[0]  # Assuming file name is table name

            # Insert data into the table
            for row in json_data:
                # Extract schema information from the JSON data
                column_names = row.keys()  # Assuming first item contains column names

                # Generate the CREATE TABLE SQL statement
                create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
                for column_name in column_names:
                    column_data_type = column_data_types.get(type(row[column_name]).__name__, 'TEXT')
                    create_table_sql += f"{column_name} {column_data_type}, "
                create_table_sql = create_table_sql.rstrip(', ') + ")"

                # Execute the SQL statement to create the table
                db_cursor.execute(create_table_sql)

                insert_sql = f"INSERT INTO {table_name} ({', '.join(row.keys())}) VALUES ({', '.join(['?'] * len(row.values()))})"

                db_cursor.execute(insert_sql, list(row.values()))

        # Commit changes
        connection.commit()

    def load_saved_data_to_memory(self, date=""):
        try:
            # This returns the ['csv', 'postgres']
            for db_folder in os.listdir('./data'):
                # This returns the date folder from csv or tables folders from
                for date_folder_or_table_folder in os.listdir(os.path.join('./data/', db_folder)):
                    # If this is true, the next loop will return the json from csv or false, will return the table names dir
                    if bool(re.search(r'\d', date_folder_or_table_folder)):
                        self.current_working_date = date_folder_or_table_folder
                        if date != "" and date == date_folder_or_table_folder:
                            for day_folder in os.listdir(
                                    os.path.join('./data/', db_folder, date_folder_or_table_folder)):
                                self.local_data.append(
                                    os.path.join('./data/', db_folder, date_folder_or_table_folder, day_folder))
                        for day_folder in os.listdir(os.path.join('./data/', db_folder, date_folder_or_table_folder)):

                            if date == "":
                                self.local_data.append(
                                    os.path.join('./data/', db_folder, date_folder_or_table_folder, day_folder))

                    else:
                        for day in os.listdir('./data/' + db_folder + '/' + date_folder_or_table_folder):

                            self.current_working_date = day
                            if date != "" and date == day:
                                for day_folder in os.listdir(
                                        os.path.join('./data/', db_folder, date_folder_or_table_folder, day)):
                                    self.local_data.append(
                                        os.path.join('./data/', db_folder, date_folder_or_table_folder, day,
                                                     day_folder))
                            for day_folder in os.listdir(
                                    os.path.join('./data/', db_folder, date_folder_or_table_folder, day)):

                                if date == "":
                                    self.local_data.append(
                                        os.path.join('./data/', db_folder, date_folder_or_table_folder, day,
                                                     day_folder))

            return True, self.local_data
        except Exception as e:
            logging.error(e)
            return False, None

    def write_data_to_file(self, category, data, table_name=""):
        try:
            # Create the directory structure if it doesn't exist
            now = datetime.datetime.now()
            formatted_date = now.strftime("%Y-%m-%d")
            table_dir = os.path.join("./data", category, table_name, formatted_date)
            os.makedirs(table_dir, exist_ok=True)

            # Construct the file path
            file_path = os.path.join(table_dir, table_name + ".json")  # Customize the format
            if category == "csv":
                file_path = os.path.join(table_dir, "order_details.json")

            # Convert datetime.date objects to formatted strings
            formatted_data = data
            if table_name != '':
                formatted_data = []
                for row in data:
                    formatted_row = {}
                    for inner_data in row:
                        row_value = row[inner_data]

                        if isinstance(row_value, datetime.date):
                            formatted_row[inner_data] = row_value.strftime("%Y-%m-%d")
                        elif '<memory' in str(row_value):  # Handle binary data
                            formatted_row[
                                inner_data] = "No valid image" if row_value.tobytes() == b'' else row_value.tobytes()
                        else:
                            formatted_row[inner_data] = row_value

                    formatted_data.append(formatted_row)

            # Save data as JSON
            with open(file_path, "w") as json_file:
                json.dump(formatted_data, json_file, indent=4)
            return True
        except Exception as e:
            logging.error(e)
            return False

    def write_datas_to_disk(self):
        try:
            csv_result = self.write_data_to_file("csv", self.csv_data)
            for key, value in self.db_data.items():
                self.write_data_to_file("postgres", value, table_name=key)
            db_result = True
        except Exception as e:
            logging.error(e)
            db_result = False
            csv_result = False

        if db_result and csv_result:
            return True
        else:
            return False

    def extract_data_from_sources(self):
        # Gets the db data
        result_db, self.db_data = self.db.fetch_and_save_all_data()

        # Gets the csv data
        self.csv_data = self.csv.save_csv_data()

        if self.csv_data and result_db:
            return True
        else:
            return False

    # Utility to find steps
    def find_step(self, number):
        for step in self.steps:
            if step['step'] == str(number):
                return step
        return None

    def run_step(self, step):
        # This runs the function saved in the step.
        function_result = step['function']()

        # Results text depends from the function_result
        result = f'Step {step["step"]} was finished Successfully' if function_result else f'Step {step["step"]} had a error.'
        logging.info(f"\nStep: {step['step']}/{self.steps[-1]['step']}\n"
                     f" Name: {step['name']}\n"
                     f"  Description: {step['description']}\n\n"
                     f" Result: {result}\n")

        # If step contain additional steps, do them
        if len(step['additional_steps']) > 0:
            for additional_step in step['additional_steps']:
                function_result = additional_step['function']()
                result = f'Additional step {additional_step["step"]} was finished Successfully' if function_result else f'Additional step {additional_step["step"]} had a error.'
                logging.info(f"\n  Additional step: {additional_step['step']}/{step['additional_steps'][-1]['step']}\n"
                             f"   Name: {additional_step['name']}\n"
                             f"    Description: {additional_step['description']}\n\n"
                             f"   Result: {result}\n")

    def run_steps_sequentially(self):
        initial_time = time.time()
        for step in self.steps:
            self.run_step(step)
        end_time = time.time()
        formatted_time = format(end_time - initial_time, ".2f")
        logging.info(f"Time elapsed executing steps: {formatted_time} seconds")

    def run_individual_step(self, step_number=None):
        self.current_working_date = ""

        if step_number is None:
            step_choice = int(input("Enter step number to run (1, 2 or 3): "))
        else:
            step_choice = int(step_number)

        step = self.find_step(step_choice)
        if step_choice == 1 and step is not None:
            self.run_step(step)
        elif step_choice == 2 and step is not None:
            if self.db_data and self.csv_data:
                self.run_step(step)
            else:
                logging.warning("Step 1 needs to be completed first. The data it builds is empty.")
        elif step_choice == 3 and step is not None:
            self.run_step(step)
        else:
            logging.warning("Invalid step choice.")

    def retrieve_data_from_complete_db(self, table_name):
        try:
            if not os.path.exists(f'./merged_databases/merged_database_date-{self.current_working_date}.db'):
                raise Exception("Date don't match with database.")
            # Connect to the combined SQLite database
            db_connection = sqlite3.connect(f'./merged_databases/merged_database_date-{self.current_working_date}.db')
            cursor = db_connection.cursor()

            # Execute the query to retrieve all data from the "orders" table
            query = f"SELECT * FROM {table_name};"
            cursor.execute(query)
            result = cursor.fetchall()

            # Get the column names
            column_names = [desc[0] for desc in cursor.description]

            formatted_rows = []
            for row in result:
                formatted_row = {}
                for i, value in enumerate(row):
                    if isinstance(value, datetime.date):
                        formatted_row[column_names[i]] = value.strftime("%Y-%m-%d")
                    else:
                        formatted_row[column_names[i]] = value
                formatted_rows.append(formatted_row)

            # Close the cursor and connection
            cursor.close()
            db_connection.close()

            return True, formatted_rows
        except Exception as e:
            logging.error(e)
            return False, None

    def reprocess_data(self, date):
        result, data = self.find_step(2)['function'](date=date)
        if result and len(data) > 0:
            self.create_new_database(date)
        else:
            logging.info(f"Date {date} wasn't found in the data folders. Maybe wrong date?")

    def query_orders(self, date=None):

        def find_order_all_details(order, details):
            order_details = []
            for order_detail in details:
                if order_detail['order_id'] == order['order_id']:
                    order_details.append(order_detail)
            return order_details

        def find_products(details, products):
            found_products = []
            for order_detail in details:
                for product in products:
                    if product['product_id'] == order_detail['product_id']:
                        found_products.append(product)

            return found_products

        if self.current_working_date == "" or date is not None:
            date_input = input("Enter a date (YYYY-MM-DD) to load the correct database to query orders: ")
        else:
            date_input = self.current_working_date

        try:
            date_obj = datetime.datetime.strptime(date_input, '%Y-%m-%d').date()
        except ValueError:
            logging.warning("Incorrect date format. Please use YYYY-MM-DD.")
            return False
            # Handle the error or return False if needed
        else:
            if date_obj > datetime.datetime.now().date():
                logging.warning("Future date is not allowed.")
                return False
                # Handle the error or return False if needed
            else:
                self.current_working_date = date_input

                try:

                    # Initialize the data
                    _, orders = self.retrieve_data_from_complete_db('orders')
                    _, details = self.retrieve_data_from_complete_db('order_details')
                    _, products = self.retrieve_data_from_complete_db('products')

                    merged_data = []

                    if orders is None or details is None or products is None:
                        logging.warning("No data returned from database. Can't proceed.")
                        return False

                    # Now here we are merging those three into one big dict.
                    for order in orders:
                        order_details = find_order_all_details(order, details)
                        products_bought = find_products(order_details, products)

                        # This merges the products bought in the order details too.
                        for order_detail in order_details:
                            for product_bought in products_bought:
                                if order_detail['product_id'] == product_bought['product_id']:
                                    order_detail['product_bought'] = product_bought

                        formatted_order = {
                            'order': order,
                            'details': order_details
                        }
                        merged_data.append(formatted_order)

                    now = datetime.datetime.now()
                    formatted_datetime = now.strftime("%Y-%m-%d_%H-%M-%S")
                    os.makedirs('./query_output/', exist_ok=True)
                    # Save the query
                    with open(f'./query_output/query_{date_input}_generated_at_{formatted_datetime}.json',
                              "w") as json_file:
                        json.dump(merged_data, json_file, indent=4)

                    return True
                except Exception as e:
                    logging.error(e)
                    return False


class CsvInput:
    def __init__(self, data_saver):
        self.data = None
        self.data_saver = data_saver

    def save_csv_data(self):
        self.data = self.load_csv_data(csv_file_path)
        # self.data_saver.write_data_to_file("csv", self.data)
        return self.data

    def load_csv_data(self, path):
        data = []
        # Open the CSV file and read its content
        with open(path, "r") as csv_file:
            csv_reader = csv.reader(csv_file)

            # Skip the header row
            next(csv_reader)

            # Iterate through each row in the CSV
            for row in csv_reader:
                order_id = int(row[0])
                product_id = int(row[1])
                unit_price = float(row[2])
                quantity = int(row[3])
                discount = float(row[4])

                # Create a dictionary to represent the data for this row
                data_row = {
                    "order_id": order_id,
                    "product_id": product_id,
                    "unit_price": unit_price,
                    "quantity": quantity,
                    "discount": discount
                }

                # Append the data_row dictionary to the data list
                data.append(data_row)

        return data


class DbInput:
    def __init__(self, data_saver):
        self.connection = self.connect_db()
        self.data_saver = data_saver

    def fetch_and_save_all_data(self):
        try:
            # Create a cursor
            cursor = self.connection.cursor()

            # Get a list of all table names from the information schema
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
            table_names = cursor.fetchall()

            # Initialize a dictionary to store the data
            all_data = {}

            # Fetch and store data from all tables
            for table in table_names:
                table_name = table[0]
                query = f"SELECT * FROM {table_name};"
                cursor.execute(query)
                result = cursor.fetchall()

                # Get the column names
                column_names = [desc[0] for desc in cursor.description]

                # Convert datetime.date objects to strings
                formatted_rows = []
                for row in result:
                    formatted_row = {}
                    for i, value in enumerate(row):
                        if isinstance(value, datetime.date):
                            formatted_row[column_names[i]] = value.strftime("%Y-%m-%d")
                        else:
                            formatted_row[column_names[i]] = value
                    formatted_rows.append(formatted_row)

                # Store formatted rows in the dictionary
                all_data[table_name] = formatted_rows

            # Close cursor
            cursor.close()
            return True, all_data
        except Exception as e:
            logging.error(e)
            return False, None

    def connect_db(self):
        # Establish a connection to the database
        connection = psycopg2.connect(**db_params)
        return connection


def main(data_saver):
    while True:
        time.sleep(1)
        print("|- Indicium Code Challenge -|")
        print("| 1. Run steps sequentially |")
        print("| 2. Run individual step    |")
        print("| 3. Query orders           |")
        print("| 4. Reprocess past date    |")
        print("| 0. Quit                   |")
        print('|___________________________|')

        choice = input("Enter your choice: ")

        try:
            if choice == "1":
                data_saver.run_steps_sequentially()
            elif choice == "2":
                data_saver.run_individual_step()
            elif choice == "3":
                data_saver.run_individual_step(3)
            elif choice == "4":
                date = input("Please input a date (YYYY-MM-DD): ")
                data_saver.reprocess_data(date)
            elif choice == "0":
                print("Exiting the menu.")
                break
            else:
                logging.warning("Invalid choice. Please select a valid option.")
        except Exception as e:
            logging.error("Couldn't complete the task. Catastrophic error occurred.")


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Indicium Code Challenge Pipeline')
    parser.add_argument('--reprocess', help='Date for reprocessing data (YYYY-MM-DD)', required=False)
    parser.add_argument('--sequentially', help='Execute steps sequentially (1 to 3)', action='store_true',
                        required=False)
    parser.add_argument('--individually', help='Execute steps individually. Choose a step (1 to 3).', required=False)
    parser.add_argument('--query', help='Date for query orders (YYYY-MM-DD). Date can be empty too.',
                        action='store_true', required=False)
    args = parser.parse_args()

    data_saver = DataSaver()

    if args.reprocess:
        # Reprocess data for a specific date
        data_saver.reprocess_data(args.date)
    elif args.sequentially:
        data_saver.run_steps_sequentially()
    elif args.individually:
        data_saver.run_individual_step(args.individually)
    elif args.query:
        data_saver.run_individual_step(3)
    else:
        # Run the pipeline for the current day
        main(data_saver)

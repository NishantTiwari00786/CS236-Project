Hotel Booking Cancellation Analysis

This project analyzes hotel booking cancellation patterns using PySpark, PostgreSQL, and a Flask web dashboard. The analysis is divided into three phases, covering data processing, database design, and web application development.

Project Overview

The project utilizes two specific hotel booking datasets and one unified dataset to study customer behavior.

Phase 1: Spark Analysis

We used PySpark to perform Exploratory Data Analysis (EDA), specifically:

Computing monthly cancellation rates.

Calculating average prices and stay durations.

Analyzing bookings by market segment.

Identifying seasonality trends based on revenue.

Phase 2: Database Design and Population

We designed a PostgreSQL schema and populated it with the cleaned data:

Designed schemas for customer_reservations, hotel_bookings, and unified_bookings.

Loaded the data using SQL scripts (or PySpark).

Phase 3: Web UI Dashboard

We developed a Flask application connected to the PostgreSQL database to:

Visualize the data in a table format.

Implement dynamic filtering for columns (e.g., Country, Booking Status).

Serve data via REST API endpoints.

Prerequisites

Ensure the following are installed on your system:

Python 3.8 or higher

Java 8 or higher (required for Spark)

PostgreSQL (installed and running)

Git

Installation and Setup

1. Clone the Repository

git clone [https://github.com/NishantTiwari00786/CS236-Project.git](https://github.com/NishantTiwari00786/CS236-Project.git)
cd CS236-Project


2. Set Up Virtual Environment

It is recommended to use a virtual environment to manage dependencies.

python -m venv venv
source venv/bin/activate          # On macOS/Linux
# venv\Scripts\activate           # On Windows


3. Install Dependencies

Install the required libraries from the requirements.txt file.

pip install -r requirements.txt


To verify the installation, check if PySpark loads correctly:

python -c "import pyspark; print('PySpark installed successfully')"


Database Setup

The project requires a PostgreSQL database named bookings.

1. Create the Database

Run the following command in your terminal:

psql postgres -c "CREATE DATABASE bookings;"


2. Import Data

Run the SQL files located in the sql/ directory to create tables and load the data. Execute them in this order:

psql -d bookings -f sql/customer_reservations.sql
psql -d bookings -f sql/hotel_bookings.sql
psql -d bookings -f sql/unified_bookings.sql


Phase 3 Configuration (Flask App)

The web dashboard requires a .env file to connect to the database.

Navigate to the Phase 3 directory:

cd scripts/phase3


Create a file named .env in this directory.

Add the following configuration to the .env file. Update DB_USER with your local PostgreSQL username.

DB_HOST=localhost
DB_NAME=bookings
DB_USER=your_postgres_username
DB_PASS=
DB_PORT=5432


Note: To find your Postgres username, run psql postgres -c "\du" in the terminal.

Running the Application

Running Spark Analysis (Phase 1 & 2)

To run the analysis scripts from the root directory:

python scripts/phase2/spark_analysis.py


Running the Web Dashboard (Phase 3)

To start the web server:

Navigate to the script directory:

cd scripts/phase3


Run the application:

python3 app.py


Open your web browser and go to:
http://127.0.0.1:5001

Note: The application runs on port 5001 to avoid conflicts with system services on macOS.

Usage

Select Dataset: Choose between Hotel Bookings, Customer Reservations, or the Unified Dataset.

Filter Column: Select a column to filter by (e.g., country).

Filter Value: Select a specific value from the dropdown.

Fetch Data: Click the button to retrieve and display the rows.

Troubleshooting

Database Connection Error: Ensure PostgreSQL is running and the credentials in the .env file are correct.

Port Error: If port 5001 is in use, edit app.py to change the port number.

Missing Data: Verify that the SQL import commands in the "Database Setup" section ran successfully.

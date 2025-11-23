Hotel Booking Cancellation Analysis

A comprehensive Exploratory Data Analysis (EDA) project analyzing hotel booking cancellation patterns using PySpark, PostgreSQL, and Flask.

Project Overview

This project analyzes two hotel booking datasets to understand cancellation patterns and customer behavior. The project is divided into three phases:

Phase 1: Spark Analysis:

Cancellation Rates: Calculate cancellation rates for each month

Averages: Compute average price and average number of nights for each month

Monthly Bookings: Count monthly bookings by market segments

Seasonality: Identify the most popular month of the year for bookings based on revenue

Phase 2: Design and Populate Database:

Schema Design: Create a schema that matches each of the three datasets (two cleaned and one unified)

Population: Load the datasets into PostgreSQL using PySpark

Phase 3: WebUI Dashboard:

Interactive Interface: A lightweight Flask-based web application to view data

Dynamic Filtering: Filter datasets by any column attribute (e.g., Country, Booking Status)

API Integration: RESTful endpoints serving JSON data from PostgreSQL to the frontend

Installation & Setup

Prerequisites

Python 3.8 or higher

Java 8 or higher (required for Spark)

PostgreSQL installed and running

Git

1. Clone the Repository

git clone [https://github.com/NishantTiwari00786/CS236-Project](https://github.com/NishantTiwari00786/CS236-Project)
cd CS236-Project


2. Create Virtual Environment

python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate


3. Install Dependencies

pip install -r requirements.txt


4. Verify Installation

python -c "import pyspark; print('PySpark installed successfully')"


🛠️ Database Configuration (Phase 3)

Before running the dashboard, you must set up the PostgreSQL database and environment variables.

1. Setup Database & Import Data

Run the following commands to create the database and populate it with the Phase 2 SQL files:

# Create the database
psql postgres -c "CREATE DATABASE bookings;"

# Import the datasets (Ensure you are in the project root)
psql -d bookings -f sql/customer_reservations.sql
psql -d bookings -f sql/hotel_bookings.sql
psql -d bookings -f sql/unified_bookings.sql


2. Configure Environment Variables

Navigate to the Phase 3 script directory:

cd scripts/phase3


Create a .env file based on the example:

cp ../../.env.example .env  # Or create it manually


Open .env and update DB_USER with your local PostgreSQL username:

DB_HOST=localhost
DB_NAME=bookings
DB_USER=your_username_here
DB_PASS=
DB_PORT=5432


📈 Running the Analysis

1. Activate Virtual Environment

source venv/bin/activate  # On Windows: venv\Scripts\activate


2. Run Spark Analysis (Phase 1)

To run the EDA scripts:

python scripts/eda/eda.py

3. Run Spark Analysis (Phase 2)

python scripts/phase2/spark_analysis.py  


4. Running the Web Dashboard (Phase 3)

Once the database is configured, you can launch the web interface.

1. Start the Flask Server

cd scripts/phase3
python app.py


2. Access the Dashboard

Open your web browser and navigate to:
https://www.google.com/url?sa=E&source=gmail&q=http://127.0.0.1:5001

The application runs on port 5001 to avoid conflicts with macOS system services 

 

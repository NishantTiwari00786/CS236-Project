Team Members: Nishant Tiwari, Simarpal Singh



# Hotel Booking Cancellation Analysis

This project analyzes hotel booking cancellation patterns using PySpark, PostgreSQL, and a Flask web dashboard. The analysis is divided into three phases, covering data processing, database design, and web application development.

---

## Project Overview

The project uses two hotel booking datasets and one unified dataset to study customer behavior and cancellations.

### Phase 1: Spark Analysis

We used PySpark to perform Exploratory Data Analysis (EDA), specifically:
- Computing monthly cancellation rates
- Calculating average prices and stay durations
- Analyzing bookings by market segment
- Identifying seasonality trends based on revenue

### Phase 2: Database Design and Population

We designed a PostgreSQL schema and populated it with the cleaned data:
- Designed schemas for `customer_reservations`, `hotel_bookings`, and `unified_bookings`
- Loaded the data using SQL scripts (or PySpark)

### Phase 3: Web UI Dashboard

We developed a Flask application connected to the PostgreSQL database to:
- Visualize the data in a table format
- Implement dynamic filtering for columns (e.g., `country`, `booking_status`)
- Serve data via REST API endpoints used by the frontend

---

## Prerequisites

Make sure the following are installed:

- Python 3.8 or higher  
- Java 8 or higher (required for Spark)  
- PostgreSQL (installed and running)  
- Git  

---

## Installation and Setup

### 1. Clone the Repository

    git clone https://github.com/NishantTiwari00786/CS236-Project.git
    cd CS236-Project

### 2. Set Up Virtual Environment

    python -m venv venv
    source venv/bin/activate          # On macOS/Linux
    # venv\Scripts\activate           # On Windows

### 3. Install Dependencies

Install the required libraries from the `requirements.txt` file.

    pip install -r requirements.txt

To verify the installation, check if PySpark loads correctly:

    python -c "import pyspark; print('PySpark installed successfully')"

---

## Database Setup

The project requires a PostgreSQL database named `bookings`.

### 1. Create the Database

Run the following command in your terminal:

    psql postgres -c "CREATE DATABASE bookings;"

### 2. Import Data

Run the SQL files located in the `sql/` directory to create tables and load the data, in this order:

    psql -d bookings -f sql/customer_reservations.sql
    psql -d bookings -f sql/hotel_bookings.sql
    psql -d bookings -f sql/unified_bookings.sql

This creates and populates:

- `customer_reservations`
- `hotel_bookings`
- `unified_bookings`

---

## Phase 3 Configuration (Flask App)

The web dashboard uses a `.env` file to connect to the PostgreSQL database.

1. Navigate to the Phase 3 directory:

       cd scripts/phase3

2. Create a file named `.env` in this directory.

3. Add the following configuration to `.env`. Update `DB_USER` (and `DB_PASS` if needed) for your local PostgreSQL setup:

       DB_HOST=localhost
       DB_NAME=bookings
       DB_USER=your_postgres_username
       DB_PASS=
       DB_PORT=5432

To find your PostgreSQL username, you can run:

    psql postgres -c "\du"

---

## Running the Application

### 1. Running Spark Analysis (Phase 1 & 2)

From the project root directory:

    python scripts/phase2/spark_analysis.py


### 2. Running the Web Dashboard (Phase 3)

1. Navigate to the Phase 3 directory:

       cd scripts/phase3

2. Start the Flask application:

       python3 app.py

3. Open your web browser and go to:

       http://127.0.0.1:5001

The application runs on port `5001` to avoid conflicts with other services on macOS.

---

## Usage

On the web dashboard:

- **Select Dataset**: Choose between *Hotel Bookings*, *Customer Reservations*, or the *Unified Dataset*.
- **Filter Column**: Select a column to filter by (for example, `country` or `booking_status`).
- **Filter Value**: Select a specific value from the dropdown. These values are loaded from PostgreSQL using a `/api/distinct_values` endpoint.
- **Fetch Data**: Click the button to send a request to `/api/bookings` and display the matching rows in the table.

---

## Troubleshooting

**Database connection error**

- Make sure PostgreSQL is running.
- Check that the `.env` file values (`DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASS`, `DB_PORT`) are correct.

**Port already in use**

- If port `5001` is in use, open `app.py` and change the line:

      app.run(debug=True, port=5001)

  to another free port, for example `5002`.

**Missing or empty data**

- Verify that the SQL import commands in the “Database Setup” section ran successfully.
- You can check the tables directly with:

      psql -d bookings
      \dt

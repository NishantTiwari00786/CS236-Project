# Hotel Booking Cancellation Analysis

A comprehensive Exploratory Data Analysis (EDA) project analyzing hotel booking cancellation patterns using Pyspark

## Project Overview

This project analyzes two hotel booking datasets to understand cancellation patterns and customer behavior. Currently, we are on Phase 2. The analysis includes:

- **Spark Analysis**: 
  - **Cancellation Rates**: Calculate cancellation rates for each month
  - **Averages**: Compute average price and average number of nights for each month
  - **Monthly Bookings**: Count monthly bookings by market segments
  - **Seasonality**: Identify the most popular month of the year for bookings based on revenue
- **Deign and Populate Database**:
  - **Schema Design**: Create a schema that matches each of the three datasets (two cleaned and one unified)
  - **Population**: Load the datasets into PostgreSQL using PySpark

## Installation & Setup

### Prerequisites

- Python 3.8 or higher
- Java 8 or higher (required for Spark)
- Git

### 1. Clone the Repository

```bash
git clone https://github.com/NishantTiwari00786/CS236-Project
cd CS236-Project
```

### 2. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Verify Installation

```bash
python -c "import pyspark; print('PySpark installed successfully')"
```

## 📈 Running the Analysis

### 1. Activate Virtual Environment

```bash
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 2. Run Spark Analysis

```bash
python scripts/phase2/spark_analysis.py   

# Hotel Booking Cancellation Analysis

A comprehensive Exploratory Data Analysis (EDA) project analyzing hotel booking cancellation patterns using Pyspark

## Project Overview

This project analyzes two hotel booking datasets to understand cancellation patterns and customer behavior. Currently, we are on Phase 1. The analysis includes:

- **Univariate Analysis**: Individual variable distributions and patterns
- **Bivariate Analysis**: Relationships between variables
- **Customer Behavior Clustering**: Identification of distinct customer types
- **Geographic Analysis**: Country-specific booking patterns
- **Seasonal Analysis**: Monthly and seasonal trends

We also provide the cleaned + merged dataset.

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

### 2. Run Univariate Analysis

```bash
python scripts/eda/univariate.py > reports/phase1/univariate/logs/univariate_results.txt 2>&1
```

### 3. Run Bivariate Analysis

```bash
python scripts/eda/bivariate.py > reports/phase1/bivariate/logs/bivariate_results.txt 2>&1
```

### 4. View Results

- **Analysis Results**: Check `reports/phase1/*/logs/*_results.txt`
- **Visualizations**: Check `reports/phase1/*/figures/` directory
- **System Logs**: Check `logs/` directory for Spark logs

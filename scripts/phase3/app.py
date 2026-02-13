from flask import Flask, render_template, request, jsonify
import psycopg2
import os
from dotenv import load_dotenv # to load .env variables

#  Calculate the path to the project root to find .env 
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "../../"))

# Load the .env file from the project root instead of the current folder
load_dotenv(os.path.join(project_root, ".env")) 

app = Flask(__name__)


# function to get a connection to the PostgreSQL database, using the environment variables
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        database=os.getenv('DB_NAME', 'bookings'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASS'),
        port=os.getenv('DB_PORT', '5432')
    )


# when user visits the home page, this function runs the index html to display the web page
@app.route('/')
def index():
    return render_template('index.html')


# API route to get data from the database with optional filtering
@app.route('/api/bookings', methods=['GET'])
def get_data():
    dataset = request.args.get('dataset')        # table to read(which are 3 sql files we have)
    filter_column = request.args.get('filter_column')  # column to filter
    filter_value = request.aret('filter_value')    # value to filter

    conn = get_db_connection()
    cur = conn.cursor()

    query = f"SELECT * FROM {dataset}" # it selects everything from the selected table 
    
    # if user selects column and value, then it will call this query to display that
    if filter_column and filter_value:
        query += f" WHERE {filter_column} = %s"
        cur.execute(query, (filter_value,))
    else:
        cur.execute(query)

    # once it gets the data, it will convert it dict format to send it to the frontend
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    result = [dict(zip(columns, row)) for row in rows]

    cur.close()
    conn.close()

    return jsonify(result)


# to populate the filter values dropdown based on the selected column
# then it returns the distinct values for that column
# and return the list of values as json response
# this is called when user changes the filter column
@app.route('/api/distinct_values', methods=['GET'])
def get_distinct_values():
    dataset = request.args.get('dataset')
    column = request.args.get('column')

    if not dataset or not column:
        return jsonify({"error": "dataset and column are required"}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    query = f"SELECT DISTINCT {column} FROM {dataset} ORDER BY {column} LIMIT 100;"
    cur.execute(query)
    rows = cur.fetchall()

    cur.close()
    conn.close()

    values = [r[0] for r in rows if r[0] is not None]
    return jsonify(values)


if __name__ == '__main__':
    app.run(debug=True, port=5001) # it shows the detailed page for debugging


# reference: https://www.geeksforgeeks.org/python/flask-tutorial/ - followed this tutorial to create the flask app structure with api routes
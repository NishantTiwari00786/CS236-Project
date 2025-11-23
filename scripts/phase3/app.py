from flask import Flask, render_template, request, jsonify
import psycopg2

app = Flask(__name__) # iniatilizing the flask app

# Database connection setup; setting up function to get  postgreSQL connection, same as in phase2. 

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="bookings",
        user = "postgres",
        password = "postgres"
        port = 5432
    )
    
    
# app route, the main ui page. 
@app.route('/')
def index():
    return render_template('index.html')

# Api route, the get data is used to fetch data from the database based on query parameters.
@app.route('/api/bookings', methods=['GET'])

def get_data():
    dataset = request.args.get('dataset') # table to read
    filter_column = request.args.get('filter_column') # column to filter
    filter_value = request.args.get('filter_value') # value to filter
    
    connection = get_db_connection()    # starting connection
    cursor = connection.cursor() # cursor for executing queries
    
    # Basic SQL query construction
    
    query = f"SELECT * FROM {dataset}"
    
    # used when user asks for specific filtering 
    
    if filter_column and filter_value:
        query += f" WHERE {filter_column} = %s"
        cursor.execute(query, (filter_value,))
    else:
        cursor.execute(query)
        
    # fetching both rows and columns     
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    
    # converting rows to list of dictionaries to get readable format
    
    result = [dict(zip(columns, row)) for row in rows]
    
    # closing the connectin and cursor
    cursor.close()
    connection.close()
    return jsonify(result)


# running the app
if __name__ == '__main__':
    app.run(debug=True)
    

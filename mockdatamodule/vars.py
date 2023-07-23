# vars.py

# Type of the database ('postgres' or 'mysql')
db_type = 'mysql'

# Size of the data to generate in GB and insert to table
size_in_gb = 1

# Database connection details
db_host = ""  # Writer endpoint in rds console

# Default port for PostgreSQL is 5432 and for MySQL is 3306
db_port = "3306"  

db_name = ""  # Need to create a db with the given name on initialization
db_username = ""  # "admin" for MySQL, "postgres" for postgres
db_password = ""
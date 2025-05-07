import MySQLdb
# Database configuration
db_config = {
'host': 'localhost',
'user': 'root',
'passwd': '19110269',
'db': 'sakila', # Here you select Sakila database
}
# }
# Create a connection to the database
conn = MySQLdb.connect(**db_config)
import sys

sql_statements = {"sql_db_bakery_cakes": "select cake_id, flavor from cakes;",
                  "sql_db_bakery_customers": "select * from customers;",
                  "sql_db_bakery_orders": "select * from orders where pickup_date = '{}';".format(sys.argv[3]) }
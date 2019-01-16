import sys

sql_statements = {"sql_db_bakery_cakes": "select cake_id, flavor from cakes;",
                  "sql_db_bakery_customers": "select customer_id, first_name, last_name, phone, street_address, city, zip_code, referrer_id from customers;",
                  "sql_db_bakery_orders": "select order_id, cake_id, customer_id, pickup_date from orders where pickup_date = '{}';".format(sys.argv[3]) }
import psycopg2
import logging
from datetime import datetime
import os
import pandas as pd

current_time = datetime.now()
fcurrent_time = current_time.strftime("%Y-%m-%d-%H-%M-%S")
log_file = os.path.join("D:\\Automation\\Logging_information\\", f"Pricing_automation_{fcurrent_time}.log")
logging.basicConfig(filename=log_file, level=logging.DEBUG)


# Connect to your PostgreSQL database
def connect_to_postgres(dbname, user, password, host, port):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        logging.info("Connected to the PostgreSQL database successfully!")
        return conn
    
    except psycopg2.Error as e:
        logging.error("Unable to connect to the database")
        logging.error(e)
        return None


def read_data_into_table(connection, df):
    # replace the company_df with P21_folder

    main_df = pd.DataFrame() 

    main_df = df.copy()

    cursor = connection.cursor()

    for index, row in main_df.iterrows():
        supplier_part_no = row["supplier_part_no"]
        clean_sup_part_no = row["clean_sup_part_no"]
        supplier_id = row["supplier_id"]
        item_prefix = row["item_prefix"]
        item_id = row["item_id"]
        clean_item = row["clean_item"]
        product_type = row["product_type"]
        on_price_book_flag = row["on_price_book_flag"]
        cln_location_cnt = row["cln_location_cnt"]
        no_of_suppliers = row["no_of_suppliers"]
        no_of_locations = row["no_of_locations"]
        buyable_locs = row["buyable_locs"]
        sellable_locs = row["sellable_locs"]
        delete_locs = row["delete_locs"]
        discontinued_locs = row["discontinued_locs"]
        prod_groups = row["prod_groups"]
        prod_grps = row["prod_grps"]
        sales_disc_grp = row["sales_disc_grp"]
        sales_disc_grps = row["sales_disc_grps"]
        purch_disc_grp = row["purch_disc_grp"]
        purch_disc_grps = row["purch_disc_grps"]
        std_cost_updates = row["std_cost_updates"]
        std_cost_update_amt = row["std_cost_update_amt"]
        discrepancy_type = row["discrepancy_type"]

        # SQL query to insert data into the table
        sql = """
        INSERT INTO your_table_name (
            supplier_part_no, clean_sup_part_no, supplier_id, item_prefix, item_id, clean_item, product_type, 
            on_price_book_flag, cln_location_cnt, no_of_suppliers, no_of_locations, buyable_locs, sellable_locs, 
            delete_locs, discontinued_locs, prod_groups, prod_grps, sales_disc_grp, sales_disc_grps, purch_disc_grp, 
            purch_disc_grps, std_cost_updates, std_cost_update_amt, discrepancy_type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Execute the SQL query with the data from the current row
        cursor.execute(sql, (
            supplier_part_no, clean_sup_part_no, supplier_id, item_prefix, item_id, clean_item, product_type, 
            on_price_book_flag, cln_location_cnt, no_of_suppliers, no_of_locations, buyable_locs, sellable_locs, 
            delete_locs, discontinued_locs, prod_groups, prod_grps, sales_disc_grp, sales_disc_grps, purch_disc_grp, 
            purch_disc_grps, std_cost_updates, std_cost_update_amt, discrepancy_type
        ))

    
    cursor.close()
        
        
# export the csv from the database
def export_table_to_csv(connection, table_name, output_file):
    try:
        cursor = connection.cursor()
        with open(output_file, 'w') as f:
            cursor.copy_expert(f"COPY {table_name} TO STDOUT WITH CSV HEADER", f)
        logging.info(f"Data from table '{table_name}' successfully exported to '{output_file}'")
        
    except psycopg2.Error as e:
        logging.error(f"Error exporting data from table '{table_name}' to CSV file")
        logging.error(e)

        raise ValueError(e)




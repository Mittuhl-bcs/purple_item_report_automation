import psycopg2
import logging
from datetime import datetime
import os
import pandas as pd
import csv


current_time = datetime.now()
fcurrent_time = current_time.strftime("%Y-%m-%d-%H-%M-%S")
log_file = os.path.join("D:\\purple_items_report_automation\\Logging_information\\", f"Pricing_automation_{fcurrent_time}.log")
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



def read_data_into_table(connection, df, new_loop):

    if new_loop == "yes":

        cursor = connection.cursor()
        # SQL query to delete existing data into the table
        sqld = """
        delete from purple_items;
        """

        # Execute the SQL query with the data from the current row
        cursor.execute(sqld)

        connection.commit()
        cursor.close()


    # replace the company_df with P21_folder
    main_df = pd.DataFrame() 

    main_df = df[df["discrepancy_types"] != "All right"]
    main_df.loc[:, 'supplier_id'] = main_df['supplier_id'].astype(str)
    main_df.loc[:, 'last_price_update'] = main_df['last_price_update'].astype(str)

    exclude_ids = ["130001", "130014", "185447", "130020", "130026", "130027", "130029", "130031", "130033", "130007", "130036", "130039", "130040", "130041", "130006"]
    filtered_df = main_df[~main_df['supplier_id'].isin(exclude_ids)]

    main_df = df.copy()

    cursor = connection.cursor()

    for index, row in filtered_df.iterrows():
        supplier_part_no = row["supplier_part_no"]
        clean_sup_part_no = row["clean_sup_part_no"]
        supplier_id = row["supplier_id"]
        item_prefix = row["item_prefix"]
        item_id = row["item_id"]
        clean_item = row["clean_item"]
        product_type = row["product_type"]
        on_price_book_flag = row["on_price_book_flag"]
        supplier_list = row["supplier_list"]
        supplier_cost = row["supplier_cost"]
        p1 = row["p1"]
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
        restricted = row["restricted"]
        loc_cost_updates = row["loc_cost_updates"]
        discrepancy_type = row["discrepancy_types"]

        # SQL query to insert data into the table
        sql = """
        INSERT INTO purple_items (
            supplier_part_no, clean_sup_part_no, supplier_id, item_prefix, item_id, clean_item, product_type, 
            on_price_book_flag, supplier_list, supplier_cost, p1, cln_location_cnt, no_of_suppliers, no_of_locations, buyable_locs, sellable_locs, 
            delete_locs, discontinued_locs, prod_groups, prod_grps, sales_disc_grp, sales_disc_grps, purch_disc_grp, 
            purch_disc_grps, restricted, loc_cost_updates, discrepancy_type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Execute the SQL query with the data from the current row
        cursor.execute(sql, (
            supplier_part_no, clean_sup_part_no, supplier_id, item_prefix, item_id, clean_item, product_type, 
            on_price_book_flag,  supplier_list, supplier_cost, p1, cln_location_cnt, no_of_suppliers, no_of_locations, buyable_locs, sellable_locs, 
            delete_locs, discontinued_locs, prod_groups, prod_grps, sales_disc_grp, sales_disc_grps, purch_disc_grp, 
            purch_disc_grps, restricted, loc_cost_updates, discrepancy_type
        ))

    
    cursor.close()
    print("Written data into Postgres Db")
        
        
# export the csv from the database
def export_table_to_csv(connection, table_name, output_file):
    try:
            cursor = connection.cursor()

            with open(output_file, 'w', encoding='utf-8', newline='') as file:
                csv_writer = csv.writer(file)

                            
                # Fetch data from the table and column headers
                cursor.execute(f"SELECT * FROM {table_name}")
                rows = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                
                # Write column headers
                csv_writer.writerow(column_names)

                # Write rows
                for row in rows:
                    csv_writer.writerow([
                        str(cell).encode('utf-8', errors='replace').decode('utf-8').replace('?', 'Error character')
                        for cell in row
                    ])
                    
    except psycopg2.Error as e:
        logging.error(f"Error exporting data from table '{table_name}' to CSV file")
        logging.error(e)

        raise ValueError(e)




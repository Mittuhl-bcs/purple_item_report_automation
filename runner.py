# this is main file that will be used for running other files for DB connection, data processing
import Pgs_connector as pgs
import processor as pmauto
import postgres_stats_update as pgsstats
from datetime import datetime 
import argparse, sys
import logging
import os
import json
import mailer
import time


dbname = 'BCS_items'
user = 'postgres'
password = 'post@BCS'
host = 'localhost' 
port = '5432'  # Default PostgreSQL port is 5432


current_time = datetime.now()
day = current_time.day
month =  current_time.strftime("%b")
year = current_time.year


def runner_main(new_loop):

    mapper = pmauto.processor()
    df = mapper.main()
    
    current_time = datetime.now()
    day = current_time.day
    month = current_time.strftime("%b")
    year = current_time.year

    # database table name and output file name
    table_name = "purple_items"
    output_file = f"D:\\Temp_items_discrepancy_reports\\Discrepancies - Purple items - Price matching report {day}-{month}-{year}.csv"


    columns_str = ['item_id', 'supplier_part_no', 'clean_sup_part_no', 'supplier_id', 'clean_item', 'short_code']
    df[columns_str] = df[columns_str].astype(str)


    conn = pgs.connect_to_postgres(dbname, user, password, host, port)
    pgs.read_data_into_table(conn, df, new_loop)
    pgs.export_table_to_csv(conn, table_name, output_file)
    conn.close()

    conn_stat = pgsstats.connect_to_postgres(dbname, user, password, host, port)

    # read the stats into the stats table
    pgsstats.read_data_into_table(conn_stat, output_file)
    conn_stat.close()

    conn_stat = pgsstats.connect_to_postgres(dbname, user, password, host, port)

    # read the stats into the stats table
    pgsstats.read_data_into_table(conn_stat, output_file)
    conn_stat.close()
    # Send mails to the recipients with the attachments
    # mailresult = mailer.send_email(output_file)
    
    # give a final output
    #if mailresult == True:
    #    print("Process fininshed. Mails have been sent with attachement!")
        #logging.info("Process fininshed. Mails have been sent with attachement!")

# get the inputs of the file paths and store it in the json file

if __name__ == "__main__":
    start_time = time.time()

    parser = argparse.ArgumentParser(description= "Mapping company and pricing files")
    parser.add_argument("--confirm", help="Give the confirmation to run the code", required=True)
    parser.add_argument("--new_loop", help="Give yes for a new loop", required=True)

    args = parser.parse_args()

    confirmation = args.confirm
    new_loop = args.new_loop
    


    if confirmation == "yes":
        # run the main function
        runner_main(new_loop)

    print("____________________________________________________________________")
    print(" ")

    timetaken = (time.time() - start_time) / 60
    print(f"Time taken for compeletion: {timetaken:2f} mins")
    print("____________________________________________________________________")
 

 
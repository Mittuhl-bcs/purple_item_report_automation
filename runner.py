# this is main file that will be used for running other files for DB connection, data processing
import Pgs_connector as pgs
import processor as pmauto
from datetime import datetime 
import argparse, sys
import logging
import os
import json
import mailer


dbname = 'BCS_items'
user = 'postgres'
password = 'post@BCS'
host = 'localhost' 
port = '5432'  # Default PostgreSQL port is 5432


current_time = datetime.now()
day = current_time.day
month =  current_time.strftime("%b")
year = current_time.year


def runner_main():

    mapper = pmauto.processor()
    df = mapper.main()
    
    current_time = datetime.now()
    day = current_time.day
    month = current_time.strftime("%b")
    year = current_time.year

    # database table name and output file name
    table_name = "blue_items"
    output_file = f"D:\\Temp_items_reports\\Discrepancies - Price matching report {day}-{month}-{year}"


    conn = pgs.connect_to_postgres(dbname, user, password, host, port)
    pgs.read_data_into_table(conn, df)
    pgs.export_table_to_csv(conn, table_name, output_file)
    conn.close()


    # Send mails to the recipients with the attachments
    # mailresult = mailer.send_email(output_file)
    
    # give a final output
    #if mailresult == True:
    #    print("Process fininshed. Mails have been sent with attachement!")
        #logging.info("Process fininshed. Mails have been sent with attachement!")

# get the inputs of the file paths and store it in the json file

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description= "Mapping company and pricing files")
    parser.add_argument("--confirm", help="Give the confirmation to run the code", required=True)

    args = parser.parse_args()

    confirmation = args.confirm
    


    if confirmation == "yes":
        # run the main function
        runner_main()

 
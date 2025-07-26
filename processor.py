# importing libraries
import pandas as pd
import numpy as np
import re
import argparse
import sys
import os
import glob
import logging
from datetime import datetime
import json
import BCS_connector_purple
import Pgs_connector



class processor():

    prefix_name = {"75F": "75F",
                   "RES": "ADEMCO, INC. (Resideo)",
                   "ALR": "ALERTON NOVAR",
                   "ALT": "Altech Corporation",
                   "APF": "Apollo-Fire",
                   "ASC": "ASCO L.P.",
                   "ASI": "ASI Controls",
                   "ACI": "Automation Components Inc (ACI)",
                   "BEL": "BELIMO AIRCONTROLS (USA), INC",
                   "BWR": "Best Wire",
                   "BAP": "BUILDING AUTOMATION PRODUCTS, INC. (BAPI)",
                   "CCS": "Contemporary Control Systems",
                   "ISC": "Controlli",
                   "DIS": "DISTECH CONTROLS",
                   "DIV": "DIVERSITECH CORPORATION",
                   "DWY": "DWYER INSTRUMENTS, INC.",
                   "FIE": "FIREYE INC.",
                   "FND": "FUNCTIONAL DEVICES, INC.",
                   "CNA": "Genuine Cable Group (GCG) (Connect Air)",
                   "GDR": "GOODRICH SALES INC.",
                   "HTM": "HEAT-TIMER CORP",
                   "HFE": "HOFFMAN ENCLOSURES INC.",
                   "HWW": "HONEYWELL INC.",
                   "HWI": "HONEYWELL INTERNATIONAL ECC US (HOFS)",
                   "HWT": "HONEYWELL THERMAL SOLUTIONS",
                   "ICM": "ICM",
                   "IDC": "IDEC CORPORATION",
                   "ICS": "Industrial Connections & Solutions, LLC",
                   "JCI": "Johnson Controls Inc",
                   "KLN": "Klein Tools, Inc.",
                   "KMC": "Kreuter (KMC) Controls",
                   "LUM": "Lumen Radio",
                   "LYX": "LynxSpring Inc.",
                   "MCO": "Macurco",
                   "MXC": "Maxicap",
                   "MAX": "MAXITROL COMPANY",
                   "MXL": "Maxline",
                   "NCG": "NU-CALGON WHOLESALER",
                   "PHX": "Phoenix Contact USA, Inc.",
                   "PLN": "PROLON",
                   "RBS": "ROBERTSHAW CONTROLS COMPANY",
                   "SAG": "SAGINAW CONTROL & ENGINEERING",
                   "SCH": "SCHNEIDER ELECTRIC BUILDINGS AMERICAS, INC",
                   "SEI": "Seitron",
                   "SEN": "SENVA, INC.",
                   "SET": "SETRA SYSTEMS, INC.",
                   "SIE": "SIEMENS INDUSTRY, INC.",
                   "SKY": "Skyfoundry",
                   "SYS": "System Sensor",
                   "TOS": "TOSIBOX, INC.",
                   "VYK": "Tridium Inc.",
                   "USM": "US Motor Nidec Motor Corp",
                   "HWA": "VULCAIN ALARM DIVISION",
                   "XYL": "Xylem",
                   "YRK": "York Chiller Parts",
                   "PFP": "Performance Pipe",
                   "PER": "Periscope",
                   "JNL": "J&L Manufacturing",
                   "RFL": "NiagaraMod",
                   "FUS": "Fuseco",
                   "SFR": "Not identified",
                   "DAN": "Danfoss"
                   }
    
    # Read the data from the BCS database
    def read_data(self):

        df = BCS_connector_purple.reader_df()

        print("Read data from the database!")        
        return df
    

    # Initiates columns
    def column_initiator(self, df):

        df["discrepancy_types"] = ""
        

        return df
    

    def modifier(self, df):

        df["supplier_cost"] = df["supplier_cost"].round(2)
        df["supplier_list"] = df["supplier_list"].round(2)
        df["p1"] = df["p1"].round(2)


        def clean_item(item):
            # Match prefix using regex
            match = re.match(r'([A-Z0-9]{3})-(.+)', item)
            if match:
                prefix = match.group(1)  # Get the prefix
                # Check if the prefix is in the class's prefix_name dictionary
                if prefix in self.prefix_name:
                    return match.group(2)  # Return the part after the prefix
            return item  # Return the item as-is if no match

        # Local function to extract prefix
        def extract_prefix(item):
            match = re.match(r'([A-Z0-9]{3})-(.+)', item)
            if match:
                prefix = match.group(1)
                # Check if the prefix is in the class's prefix_name dictionary
                if prefix in self.prefix_name:
                    return prefix
            return ""  # Return empty string if no match


        # Apply the functions to the DataFrame
        df['item_prefix'] = df['item_id'].apply(extract_prefix)
        df['clean_item'] = df['item_id'].apply(clean_item)
        
        # downloading a BUP of full file
        df.to_excel("output.xlsx", index = False)
        
        # return df
        return df


    # Checks the criteria given
    def checker(self, df):

        

        for index, row in df.iterrows():
            discrepancy_types = []

            discrepancy_flag = 0

            cost = df.loc[index, "supplier_cost"]
            listp = df.loc[index, "supplier_list"]
            p1 = df.loc[index, "p1"]

            cost = round(cost, 2)
            listp =round(listp, 2)
            p1 = round(p1, 2)


            if df.loc[index, "clean_sup_part_no"] != df.loc[index, "clean_item"]:
                discrepancy_types.append("clean SPN & clean itemid")
                discrepancy_flag = 1

            if df.loc[index, "supplier_part_no"] != df.loc[index, "short_code"]:
                discrepancy_types.append("SPN & shortcode")
                discrepancy_flag = 1
                
            if df.loc[index, "prod_grps"] != "BCS INV":
                discrepancy_types.append("product group")
                discrepancy_flag = 1

            
            if df.loc[index, "delete_locs"] > 0:
                discrepancy_types.append("Delete locations")
                discrepancy_flag = 1

            if df.loc[index, "discontinued_locs"] > 0:
                discrepancy_types.append("Discontinued locations")
                discrepancy_flag = 1

            if df.loc[index, "purch_disc_grps"] != "DEFAULT": # question : should it include Default with others, or should it be only default
                discrepancy_types.append("Purchase disc group")
                discrepancy_flag = 1

            if df.loc[index, "sales_disc_grps"] != "DEFAULT":
                discrepancy_types.append("Sales disc group")
                discrepancy_flag = 1

            if df.loc[index, "restricted"] != "N":
                discrepancy_types.append("Restricted")
                discrepancy_flag = 1

            if df.loc[index, "product_type"] != "Temp":
                discrepancy_types.append("Product type")
                discrepancy_flag = 1

            if df.loc[index, "on_price_book_flag"] != "N":
                discrepancy_types.append("on_price_book_flag")
                discrepancy_flag = 1


            p1_cal = round((cost / 0.65) * 2, 2)
            p1_com = 0

            if p1_cal < round(listp, 2):
                p1_com = listp
            else:
                p1_com = round((cost / 0.65) * 2, 2)


            if p1 != p1_com:
                discrepancy_types.append("P1")
                discrepancy_flag = 1


            shortcode = df.loc[index, "short_code"]

            if pd.notnull(shortcode):
                shortcode_cleaned = re.sub(r'[^a-zA-Z0-9\s]', "", shortcode)
                #print(shortcode_cleaned)
                
                if shortcode != df.loc[index, "clean_sup_part_no"]:     # needs to be included in query
                    discrepancy_types.append("shortcode & clean SPN")       # question: shortcode is a part of SPN?
                    discrepancy_flag = 1


            item_id = df.loc[index, "item_id"]

            if pd.notnull(item_id):
                pattern = re.compile(r'[^a-zA-Z0-9 ]')

                if pattern.search(item_id):
                    discrepancy_types.append("Item Id")
                    discrepancy_flag = 1


            # assinging values to the column
            if discrepancy_flag != 1:
                df.loc[index, "discrepancy_types"] = "All right"

            elif discrepancy_flag == 1:
                discrepancy_types.sort()
                joined_discrepany = " - ".join(discrepancy_types)

                df.loc[index, "discrepancy_types"] = joined_discrepany
            
        print("Checks have been applied")  
        # return the processed df
        return df
    

    def main(self):

        processorob = processor()
        df = processorob.read_data()

        columns_str = ['item_id', 'supplier_part_no', 'clean_sup_part_no', 'supplier_id', 'clean_item', 'short_code']
        df[columns_str] = df[columns_str].astype(str)

        print(df[df["supplier_part_no"] == "2881"])
        
        df = processorob.column_initiator(df)
        df = processorob.modifier(df)
        df = processorob.checker(df)

        return df
        
        # it should return the df
        # return df
import shutil
import pandas as pd
from pymongo import MongoClient
import json
from datetime import date, datetime, timedelta
from fastapi import FastAPI, File, UploadFile

app = FastAPI(title = "Regional AQR")

bm_uat_uri = "mongodb+srv://BenchMUat-analysis:eAIOyi1MWbs3hAP8@cluster0.hotvf.mongodb.net/benchmarkeruat?readPreference=secondary&retryWrites=true&w=majority"

def _get_collection(connection_link, dbase, collection_name):

    # Connecting to MongoDB Client
    client = MongoClient(connection_link)
    # Fetching Volume Forecasting Database
    db = client[dbase]
    # List of all available collections
    db_collections = db.list_collection_names()
    # Returning the latest sales order collection from the DB
    return db

bmaut_db = _get_collection(bm_uat_uri, "benchmarkeruat", "output_sumanth")

@app.post("/upload-file/")
async def create_upload_file(uploaded_file: UploadFile = File(...)):    
    file_location = f"./files/{uploaded_file.filename}"
    with open(file_location, "wb+") as file_object:
        shutil.copyfileobj(uploaded_file.file, file_object)    
        
    with open('./source/input_params.json', 'rb') as input_params_json:
        inp_params = json.load(input_params_json)
	
    empty_df = pd.DataFrame()
    
    if str(uploaded_file.filename).endswith('xlsx'):
    
        for i in inp_params[0]["plant_list"]:
            
            missing_sheet_list = []
            error_status = 0
            error_msg = "None"
            error_ind = "0 Errors Observed"
            run_status = 100

            try:

                read_file = pd.read_excel(file_location, sheet_name = i)

                empty_df = pd.concat([empty_df, read_file], axis = 0)

            except:
                continue

            try:

                nan_dict = read_file.isna().sum().to_dict()

            except:

                error_msg = "Unable to identify nan record count in Sheet {}".format(i)
                error_status = 1

            columns_list = read_file.columns.to_list()

            missing_columns_list = []

            mand_cols = ["DATE", "SOURCE (CC/DC/LOCAL)", "PLANT", "VENDOR/CC NAME", "CODE (FRH/PREM/ECO)", "LoB", "SKU CODE", "SKU NAME", "UOM", 
            "Total Qty Received", "PO Qty/ Sto", "Sample Qty", "Total Qty Accepted", "Total Qty Rejected", "Total Recovery Accepted %", 
            "Total  Recovery Rejected %", "Overall Accepted Qty","Overall Rejected Qty","Major Defect 1/3","Major Defect 2/3","Major Defect 3/3"]

            for n, col  in enumerate(mand_cols):

                if col not in columns_list:                    

                    err_col = columns_list[n]

                    missing_sheet_list.append(i)

                    error_msg = "Required columns missing in sheet {}. Column name {} is causing the error".format(missing_sheet_list, err_col) 
                    run_status = "110"
                    error_status = 1
                    error_ind = "Make sure column names are the same as following: {}".format(mand_cols)

                    return {"error_status": error_status, "error_cause": error_msg, "error_fix": error_ind, "run_status": run_status}

            print(read_file.isna().sum())

            column_missing_sheet = []

            date_nan_count = read_file["DATE"].isna().sum()

            print(date_nan_count, "True")

            if date_nan_count > 0:

                print(date_nan_count)
                
                column_missing_sheet.append(i)
                
                error_msg = "Dates are missing from sheet {}".format(column_missing_sheet) 
                run_status = "111"
                error_status = 1
                error_ind = "Error Occured. Please fix the problem as captured in error_cause and try again"

                break
            
            
                
            if read_file["SKU CODE"].isna().sum() > 0:

                column_missing_sheet.append(i)

                error_msg = "SKU codes are missing from sheet {}".format(column_missing_sheet) 
                RuntimeError("SKU codes missing in data")
                error_status = 1
                error_ind = "Error Occured. Please fix the problem as captured in error_cause and try again"
                run_status = "112"

                break 
                
            if read_file["SOURCE (CC/DC/LOCAL)"].isna().sum() > 0:
            
                error_msg = "Procurement source CC/DC/Local missing from sheet {}".format(column_missing_sheet) 
                run_status = "113"
                error_status = 1
                error_ind = "Error Occured. Please fix the problem as captured in error_cause and try again"

                break
    
            if read_file["PLANT"].isna().sum() > 0:

                error_msg = "Plant codes are missing from sheet {}".format(i) 
                run_status = "117"
                error_status = 1
                error_ind = "Error Occured. Please fix the problem as captured in error_cause and try again"

                break
                
            if read_file["VENDOR/CC NAME"].isna().sum() > 0:
                
                error_msg = "Vendor/CC names are missing from sheet {}".format(i) 
                run_status = "119"
                error_status = 1
                error_ind = "Error Occured. Please fix the problem as captured in error_cause and try again"

                break

            now = str(datetime.now())[:19]

            empty_df["upload_time"] = datetime.now()

            empty_df['DATE'] = empty_df['DATE'].astype(object).where(empty_df['DATE'].notnull(), None)

            bmaut_db.regional_aqr_upload_log.insert_many(empty_df.to_dict('records'))

            empty_df.to_excel("./processed_files/processed_reg_AQR_file_{}.xlsx".format(now))


    elif str(uploaded_file.filename).endswith('csv'):

        read_file = pd.read_csv(file_location)
        
    return {"error_status": error_status, "error_cause": error_msg, "error_fix": error_ind, "run_status": run_status, "unavailable_data_summary": nan_dict}

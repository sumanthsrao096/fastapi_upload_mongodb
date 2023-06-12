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
	
    print(inp_params[0]["plant_list"])
    
    empty_df = pd.DataFrame()
    
    if str(uploaded_file.filename).endswith('xlsx'):
    
        for i in inp_params[0]["plant_list"]:
    
            try:
           		
                read_file = pd.read_excel(file_location, sheet_name = i)
                print(i, '\n', read_file.columns)
                empty_df = pd.concat([empty_df, read_file], axis = 0)
                print('\n', i, '\n', empty_df)
            	
            except:
            	continue
    
        now = str(datetime.now())[:19]

        empty_df["upload_time"] = datetime.now()

        empty_df['DATE'] = empty_df['DATE'].astype(object).where(empty_df['DATE'].notnull(), None)

        bmaut_db.regional_aqr_upload_log.insert_many(empty_df.to_dict('records'))

        empty_df.to_excel("./processed_files/processed_reg_AQR_file_{}.xlsx".format(now))


    elif str(uploaded_file.filename).endswith('csv'):

        read_file = pd.read_csv(file_location)
        
    return {"info": f"file '{uploaded_file.filename}' saved at '{file_location}'"}

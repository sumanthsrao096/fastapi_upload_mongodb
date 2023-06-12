from fastapi import FastAPI, File, UploadFile, Request
import uvicorn
import shutil
import pandas as pd
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
app = FastAPI(title = "FS Regional AQR Upload")
templates = Jinja2Templates(directory="templates")

@app.get("/upload/", response_class=HTMLResponse)
async def upload(request: Request):
   return templates.TemplateResponse("uploadfile.html", {"request": request})

@app.post("/uploader/")
async def create_upload_file(file: UploadFile = File(...)):

   def save_upload_file(destination: './') -> None:
    try:
        with destination.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    finally:
        file.file.close()	
	
   if str(file.file).endswith('.xlsx'):
   	
   	read_file = pd.read_excel(file.file)
   	print(read_file)
   	
   elif str(file.file).endswith('.csv'):
   
   	read_file = pd.read_csv(file.file)
   	print(read_file)

   return {"filename": file.filename, "Result": read_file}

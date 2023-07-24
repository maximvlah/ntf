from fastapi import FastAPI, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
import shutil
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from starlette.background import BackgroundTasks
from datetime import datetime
import json
import pandas as pd
import zipfile
import uuid
import asyncio
import numpy as np
from pathlib import Path
from typing import List
from tqdm import tqdm
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

import sys
import os
import csv


# FIELDS = ["filename", "company", "date", "vendor", "date", "net_amount", "gross_amount", "tax_amount", "ean", "description", "article_id", "quantity", "unit_of_measure", "unit_price", "tax_rate", "total_price"]

FIELDS = ["filename", "receipt_number", "company", "vendor", "date", "net_amount", "gross_amount", "tax_amount", "ean", "description", "article_id", "quantity", "unit_of_measure", "unit_price", "tax_rate", "total_price"]

excel_map = {}

def unzip_file_to_directory(zip_file_path, output_directory):
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            # Create the output directory if it doesn't exist
            os.makedirs(output_directory, exist_ok=True)

            # Extract all contents of the ZIP file to the output directory
            zip_ref.extractall(output_directory)

        print("Unzipped successfully!")
    except zipfile.BadZipFile:
        print("Error: Invalid ZIP file.")
    except FileNotFoundError:
        print("Error: File not found.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def _convert_natif_to_structured(data:dict):
    """process natif.ai parser output"""

    def _extract_from_dict(d):
        if isinstance(d,dict):
            return d.get("value")
        else:
            return d

    # General information about the receipt
    
    ## Company Name
    company = data["customer"].get("name",None)
    company = _extract_from_dict(company)

    ## Vendor Name
    vendor = data["vendor"].get("name",None)
    vendor = _extract_from_dict(vendor)

    ## Date
    date = data.get("date",None)
    date = _extract_from_dict(date)

    ## Receipt number
    receipt_number = data.get("number",None)
    receipt_number = _extract_from_dict(receipt_number)

    ## Netto Total
    net_amount = data.get("net_amount",None)
    net_amount = _extract_from_dict(net_amount)

    ## Brutto Total
    gross_amount = data.get("gross_amount",None)
    gross_amount = _extract_from_dict(gross_amount)

    ## Tax Total
    tax_amount = data.get("tax_amount",None)
    tax_amount = _extract_from_dict(tax_amount)

    # Items Information
    clean_items = []
    parsed_items = data.get("line_item")
    if parsed_items and isinstance(parsed_items,list):
        
        for item in parsed_items:
            clean_item = {}

            for col in [
                "ean",
                "description",
                "article_id",
                "quantity",
                "unit_of_measure",
                "unit_price",
                "tax_rate",
                "total_price"
            ]:
                clean_item[col] = _extract_from_dict(item.get(col))

            # non_null_item = {}
            # for k,v in clean_item.items():
            #     if v:
            #         non_null_item[k] = v

            clean_items.append(clean_item)
    
    return dict(
        general=dict(
            receipt_number=receipt_number,
            company=company,
            vendor=vendor,
            date=date,
            net_amount=net_amount,
            gross_amount=gross_amount,
            tax_amount=tax_amount,
        ),
        items=clean_items
    )

def convert_parsed_results_to_structured_format(data:dict, parser:str) -> dict:
    if parser == "natif":
        return _convert_natif_to_structured(data)
    raise NotImplementedError("Unknown parser")

def flatten_receipt_data(data):
    general = data["general"]

    flattened = []
    for item in data["items"]:
        flat_item = {**general,**item}
        flattened.append(flat_item)

    return flattened


def process_files(files: List[str]):

    # now = datetime.now().strftime("%d-%m-%Y-%H-%M-%S")
    _id = str(uuid.uuid4())

    csv_path = f"{_id}.csv"

    with ThreadPoolExecutor() as executor, open(csv_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELDS)
        writer.writeheader()

        total_files = len(files)

        futures = [executor.submit(process_file, file) for file in files[:100]]

        for i, future in tqdm(
                enumerate(concurrent.futures.as_completed(futures)),
                total=total_files,
                desc="Processing Files",
                unit="file"
        ):
            try:
                data = future.result()
                if data:
                    writer.writerows(data)

            except Exception as e:
                error_message = f"Error in file: {e}"
                print(error_message, data)


    excel_path = csv_path.replace(".csv", ".xlsx")

    excel_map[_id] = excel_path

    pd.read_csv(csv_path).to_excel(excel_path,index=False)

    os.unlink(csv_path)

    return {"message": f"Saved to {excel_path}", "id": _id, "excel_path": excel_path}


def process_file(file_path: str):
    with open(file_path, 'r') as file:
        data = json.load(file)
    try:
        data = flatten_receipt_data(convert_parsed_results_to_structured_format(data, "natif"))
        data = [{**{"filename": Path(file_path).name}, **item} for item in data]
    except Exception as e:
        print(f"Error in file {file_path}: {e}")
        data = [{**{"filename": Path(file_path).name},**{field:"PARSER FAILED" for field in FIELDS}}]
    return data


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = "uploads"


@app.post("/upload/")
async def upload_file(file: UploadFile = UploadFile(...)):
    try:
        # Create the 'uploads' directory if it doesn't exist
        os.makedirs(UPLOAD_DIR, exist_ok=True)

        file_path = os.path.join(UPLOAD_DIR, file.filename)
        
        # Save the file to disk
        with open(file_path, "wb") as f:
            shutil.copyfileobj(file.file, f)

        print("Unzipping")
        unzip_file_to_directory(file_path,"temp")

        print("Processing")
        json_files = list(Path("temp").rglob("*.json"))
        json_files = [str(p) for p in json_files]

        results = process_files(json_files)
        shutil.rmtree("temp")
        shutil.rmtree(UPLOAD_DIR)
        
        return JSONResponse(content={"message": "File uploaded successfully", "filename": file.filename, "id":results["id"]})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": f"An error occurred during file upload: {e}"})

@app.get("/export/excel/{id}")
def export_excel(id:str,background_tasks: BackgroundTasks):
    response = FileResponse(id, filename=id)
    background_tasks.add_task(os.unlink, id)
    return response

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import csv
from datetime import datetime
import logging

# ---------------------------
# CONFIG
# ---------------------------
PROJECT_ID = "retail-sales-forecast-490814"
BUCKET = "jay-retail-data-pipeline"
INPUT_FILE = f"gs://{BUCKET}/raw/stores_sales_forecasting.csv"
OUTPUT_TABLE = f"{PROJECT_ID}:retail.sales_data"

# Explicit schema matching BigQuery rules
schema = 'Row_ID:STRING,Order_ID:STRING,Order_Date:DATE,Ship_Date:DATE,Sales:FLOAT,Quantity:INTEGER,Profit:FLOAT,Profit_Margin:FLOAT'

# ---------------------------
# PARSE CSV
# ---------------------------
class ParseCSV(beam.DoFn):
    def process(self, element):
        try:
            row = next(csv.reader([element]))
            
            # Skip empty or truncated lines safely
            if not row or len(row) < 21:
                return
                
            # Skip the header row explicitly
            if str(row[0]).strip().lower() in ("row id", "row_id", ""):
                return

            yield {
                "Row_ID": row[0],
                "Order_ID": row[1],
                "Order_Date": row[2],
                "Ship_Date": row[3],
                "Ship_Mode": row[4],
                "Customer_ID": row[5],
                "Customer_Name": row[6],
                "Segment": row[7],
                "Country": row[8],
                "City": row[9],
                "State": row[10],
                "Postal_Code": row[11],
                "Region": row[12],
                "Product_ID": row[13],
                "Category": row[14],
                "Sub_Category": row[15],
                "Product_Name": row[16],
                "Sales": row[17],
                "Quantity": row[18],
                "Discount": row[19],
                "Profit": row[20]
            }
        except Exception as e:
            logging.warning(f"Failed to parse row: {e}")

# ---------------------------
# CLEAN DATA
# ---------------------------
class CleanData(beam.DoFn):
    def process(self, row):
        try:
            # Strip spaces, quotes, currency markers to avoid type casting failures
            def clean_numeric(val):
                if not val:
                    return "0.0"
                return str(val).replace("$", "").replace(",", "").replace('"', '').strip()

            raw_sales = clean_numeric(row.get("Sales"))
            raw_profit = clean_numeric(row.get("Profit"))
            raw_quantity = clean_numeric(row.get("Quantity"))

            sales = float(raw_sales) if raw_sales else 0.0
            profit = float(raw_profit) if raw_profit else 0.0
            quantity = int(float(raw_quantity)) if raw_quantity else 0

            # Dynamic date parser that works with your specific CSV layouts
            def parse_date(d):
                if not d:
                    return None
                d = str(d).strip().replace('"', '')
                
                # Broad matrix of date patterns covering your input data formats
                formats = (
                    "%m-%d-%Y", "%d-%m-%Y", "%Y-%m-%d",
                    "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d",
                    "%m-%d-%y", "%d-%m-%y"
                )
                for fmt in formats:
                    try:
                        return datetime.strptime(d, fmt).strftime("%Y-%m-%d")
                    except ValueError:
                        continue
                
                # Fallback: if it still doesn't match, log it and return None
                logging.warning(f"Unable to parse date string: {d}")
                return None

            order_date = parse_date(row.get("Order_Date"))
            ship_date = parse_date(row.get("Ship_Date"))

            # Calculate profit margin safely
            profit_margin = profit / sales if sales != 0 else 0.0

            # Formulate structured record for BigQuery matching the exact target layout
            cleaned_record = {
                "Row_ID": str(row.get("Row_ID", "")),
                "Order_ID": str(row.get("Order_ID", "")),
                "Order_Date": order_date,
                "Ship_Date": ship_date,
                "Sales": sales,
                "Quantity": quantity,
                "Profit": profit,
                "Profit_Margin": profit_margin
            }

            yield cleaned_record

        except Exception as e:
            logging.error(f"Row drop occurred during cleanup processing: {e}")

# ---------------------------
# MAIN PIPELINE RUNNER
# ---------------------------
def run():
    pipeline_options = PipelineOptions(
        runner="DataflowRunner",
        project=PROJECT_ID,
        temp_location=f"gs://{BUCKET}/temp",
        region="us-central1"
    )

    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read CSV" >> beam.io.ReadFromText(INPUT_FILE, skip_header_lines=1)
            | "Parse CSV" >> beam.ParDo(ParseCSV()) 
            | "Clean Data" >> beam.ParDo(CleanData())
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                OUTPUT_TABLE,
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
import os
import zipfile
from google.cloud import storage
import pandas as pd
import functions_framework


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def etl_pipeline(cloud_event):
    temp_dir = "/tmp/etl_output"
    try:
        # Extract event data
        data = cloud_event.data
        bucket_name = data["bucket"]
        print(f"Processing files in bucket: {bucket_name}")


        # List of required input files
        file_paths = [
            "Customer.csv",
            "Product.csv",
            "Sales.csv",
            "MarketCampaign.csv",
            "Delivery.csv",
        ]


        # Create a storage client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)


        # Read each required file into a DataFrame
        dataframes = {}
        for file in file_paths:
            blob = bucket.blob(file)
            if not blob.exists():
                raise FileNotFoundError(f"File {file} not found in bucket {bucket_name}.")
            data = blob.download_as_string()
            dataframes[file] = pd.read_csv(pd.compat.StringIO(data.decode("utf-8")))


        # Extract individual DataFrames
        customers_df = dataframes["Customer.csv"]
        products_df = dataframes["Product.csv"]
        sales_df = dataframes["Sales.csv"]
        campaigns_df = dataframes["MarketCampaign.csv"]
        deliveries_df = dataframes["Delivery.csv"]


        # Transform: Create additional attributes
        # Aggregate Sales Data for Fact Table
        sales_agg = sales_df.groupby("SalesID").agg(
            TotalSaleAmount=("SaleAmount", "sum"),
            TotalDiscountApplied=("DiscountApplied", "mean"),
            TotalDeliveryFee=("DeliveryFee", "sum"),
        ).reset_index()


        # Merge sales with dimensions for Fact Table
        sales_fact = sales_df.merge(sales_agg, on="SalesID", how="left")
        sales_fact_table = sales_fact[
            [
                "SalesID",
                "ProductID",
                "CustomerID",
                "CampaignID",
                "DeliveryID",
                "OrderDate",
                "TotalSaleAmount",
                "TotalDiscountApplied",
                "TotalDeliveryFee",
            ]
        ]


        # Create Time Dimension Table
        unique_dates = pd.to_datetime(
            pd.concat([sales_df["OrderDate"], sales_df["DeliveryDate"]])
        ).dropna().unique()
        time_dim = pd.DataFrame({"Date": sorted(unique_dates)})
        time_dim["TimeID"] = time_dim["Date"].dt.strftime("%Y%m%d")
        time_dim["Year"] = time_dim["Date"].dt.year
        time_dim["Month"] = time_dim["Date"].dt.month
        time_dim["Day"] = time_dim["Date"].dt.day
        time_dim["WeekDay"] = time_dim["Date"].dt.day_name()


        # Save DataFrames as CSV
        output_files = {
            "Customer_Dimension": customers_df,
            "Product_Dimension": products_df,
            "Sales_Fact": sales_fact_table,
            "Time_Dimension": time_dim,
            "Campaign_Dimension": campaigns_df,
            "Delivery_Dimension": deliveries_df,
        }


        # Create a temporary directory
        os.makedirs(temp_dir, exist_ok=True)


        for table_name, df in output_files.items():
            output_path = os.path.join(temp_dir, f"{table_name}.csv")
            df.to_csv(output_path, index=False)
            print(f"Saved {table_name} to {output_path}")


        # Create a ZIP file
        zip_file_path = os.path.join(temp_dir, "etl_output.zip")
        with zipfile.ZipFile(zip_file_path, "w") as zipf:
            for file_name in os.listdir(temp_dir):
                file_path = os.path.join(temp_dir, file_name)
                if file_name.endswith(".csv"):  # Only include CSV files
                    zipf.write(file_path, arcname=file_name)


        print(f"Created ZIP file at {zip_file_path}")


        # Upload the ZIP file back to the GCS bucket
        output_blob_name = "etl_output.zip"
        output_blob = bucket.blob(output_blob_name)
        output_blob.upload_from_filename(zip_file_path)


        print(f"Uploaded ZIP file to GCS bucket: {bucket_name} as {output_blob_name}")


    except Exception as e:
        print(f"Error during ETL process: {e}")


    finally:
        # Clean up the temporary directory
        try:
            if os.path.exists(temp_dir):
                for file_name in os.listdir(temp_dir):
                    os.remove(os.path.join(temp_dir, file_name))
                os.rmdir(temp_dir)
            print("Cleaned up temporary directory.")
        except Exception as cleanup_error:
            print(f"Error during cleanup: {cleanup_error}")

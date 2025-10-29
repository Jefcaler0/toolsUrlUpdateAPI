import asyncio
import datetime
import logging
import os
from urllib.parse import urlparse

import aiohttp
import pandas as pd
import pyodbc
import requests
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("process.log"),  # Log file
        logging.StreamHandler()  # Console output
    ]
)


class ConfigLoader:
    """Handles loading configuration variables from environment files."""

    def __init__(self):
        load_dotenv()
        self.db_server = os.getenv("DB_SERVER")
        self.db_database = os.getenv("DB_DATABASE")
        self.db_username = os.getenv("DB_USERNAME")
        self.db_password = os.getenv("DB_PASSWORD")
        self.upload_url = os.getenv("UPLOAD_URL")
        self.api_key = os.getenv("API_KEY")
        self.request_delay = float(os.getenv("REQUEST_DELAY", 1.0))
        self.image_download_path = os.getenv("IMAGE_DOWNLOAD_PATH", os.path.join(os.getcwd(), "images"))

        # Ensure the image download directory exists
        os.makedirs(self.image_download_path, exist_ok=True)


class DatabaseConnection:
    """Handles database connections and queries."""

    def __init__(self, config: ConfigLoader):
        self.conn_str = (
            f'DRIVER={{SQL Server}};'
            f'SERVER={config.db_server};'
            f'DATABASE={config.db_database};'
            f'UID={config.db_username};'
            f'PWD={config.db_password};'
        )
        self.connection = pyodbc.connect(self.conn_str)
        self.cursor = self.connection.cursor()

    def fetch_data(self, query: str):
        logging.info("Fetching data from database...")
        self.cursor.execute(query)
        return pd.DataFrame.from_records(self.cursor.fetchall(), columns=[desc[0] for desc in self.cursor.description])

    def close(self):
        self.cursor.close()
        self.connection.close()
        logging.info("Database connection closed.")


class FileUploader:
    """Handles the file upload process asynchronously."""

    def __init__(self, config: ConfigLoader):
        self.upload_url = config.upload_url
        self.api_key = config.api_key
        self.request_delay = config.request_delay
        self.image_download_path = config.image_download_path
        self.max_retries = 3  # Maximum retry attempts for failed uploads

    def download_image(self, url, product_id, media_id):
        """Downloads the image from the provided URL and saves it locally with a unique filename."""
        original_filename = os.path.basename(urlparse(url).path)
        new_filename = f"{product_id}_{media_id}_{original_filename}"
        file_path = os.path.join(self.image_download_path, new_filename)

        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                with open(file_path, 'wb') as file:
                    for chunk in response.iter_content(1024):
                        file.write(chunk)
                logging.info(f"Image downloaded and saved as: {file_path}")
                return file_path, new_filename
            else:
                logging.error(f"Failed to download image: {url}, Status Code: {response.status_code}")
                return None, None
        except Exception as e:
            logging.error(f"Exception while downloading image {url}: {str(e)}")
            return None, None

    async def upload_file(self, session: aiohttp.ClientSession, row):
        logging.info(
            f"Downloading image from URL: {row['URL']} for Product ID: {row['ProductId']}, Media ID: {row['MediaId']}")
        file_path, file_name = self.download_image(row['URL'], row['ProductId'], row['MediaId'])

        if not file_path:
            return "Error", "Failed to download image", ""

        current_date = datetime.datetime.now(datetime.UTC).isoformat()

        data = {
            "TenantId": "2",
            "EntityType": "product",
            "EntityId": row["ProductId"],
            "MediaResourceId": row["MediaResourceId"],
            "Order": row["Order"],
            "InternalCode": row["ProductId"],
            "MediaType": "image",
            "Date": current_date
        }

        form = aiohttp.FormData()
        with open(file_path, "rb") as file:
            form.add_field("FormFile", file.read(), filename=file_name, content_type=row["ContentType"])
        for key, value in data.items():
            form.add_field(key, str(value))

        headers = {
            "Accept": "application/json",
            "api-key": self.api_key
        }

        for attempt in range(self.max_retries):
            try:
                async with session.post(self.upload_url, headers=headers, data=form, timeout=10) as response:
                    response_text = await response.text()
                    if response.status == 200:
                        logging.info(
                            f"Upload successful for Product ID: {row['ProductId']}, Media ID: {row['MediaId']}")
                        return "Success", "Uploaded successfully", response_text
                    else:
                        logging.error(
                            f"Upload failed (Attempt {attempt + 1}/{self.max_retries}) for Product ID: {row['ProductId']}, Media ID: {row['MediaId']}. Response: {response_text}")
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
            except asyncio.TimeoutError:
                logging.error(
                    f"TimeoutError (Attempt {attempt + 1}/{self.max_retries}) for Product ID: {row['ProductId']}, Media ID: {row['MediaId']}")
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logging.exception(
                    f"Exception occurred (Attempt {attempt + 1}/{self.max_retries}) during upload for Product ID: {row['ProductId']}, Media ID: {row['MediaId']}")
                await asyncio.sleep(2 ** attempt)
        return "Error", "Max retries reached", ""


class ProcessManager:
    """Coordinates the entire process flow asynchronously."""

    def __init__(self):
        self.config = ConfigLoader()
        self.db = DatabaseConnection(self.config)
        self.uploader = FileUploader(self.config)

    async def run(self):
        logging.info("Starting process...")
        query = """
                SELECT TOP 100  p.ProductId, p.sku, p.SystemCode, pm.LinkId,pm.[Order],m.MediaId, m.URL, m.ParentId, m.ImageType, m.MediaResourceId,
                m.CompanyId, m.FileUrlBase64, m.[ContentType], p.Status [productstatus], pm.Status [linkproductmediastatus], m.Status [mediastatus]
                 from mdl04.tblProduct p
                inner JOIN mdl04.linkProductMedia pm ON p.ProductId = pm.ProductId
                INNER JOIN  mdl04.tblMedia m ON pm.MediaId = m.MediaId
                WHERE m.MediaResourceId = '68920485-d222-4ff0-b947-e0340d77b56a' AND p.Status=1 AND pm.Status=1 AND m.Status=1 AND m.ParentId is NULL
                order BY p.createddate ASC
        """

        df = self.db.fetch_data(query)
        total_records = len(df)
        async with aiohttp.ClientSession() as session:
            tasks = [self.uploader.upload_file(session, row) for _, row in df.iterrows()]
            results = await asyncio.gather(*tasks)

        logging.info(f"Total records processed: {total_records}")
        df.to_excel("output.xlsx", index=False, engine="openpyxl")

        self.cleanup()
        logging.info("Process completed successfully. Results saved in output.xlsx")

    def cleanup(self):
        self.db.close()


if __name__ == "__main__":
    asyncio.run(ProcessManager().run())
import asyncio
import datetime
import logging
import os
from urllib.parse import urlparse

import aiohttp
import pandas as pd
import pyodbc
import requests
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("process.log"),  # Log file
        logging.StreamHandler()  # Console output
    ]
)


class ConfigLoader:
    """Handles loading configuration variables from environment files."""

    def __init__(self):
        load_dotenv()
        self.db_server = os.getenv("DB_SERVER")
        self.db_database = os.getenv("DB_DATABASE")
        self.db_username = os.getenv("DB_USERNAME")
        self.db_password = os.getenv("DB_PASSWORD")
        self.upload_url = os.getenv("UPLOAD_URL")
        self.api_key = os.getenv("API_KEY")
        self.request_delay = float(os.getenv("REQUEST_DELAY", 1.0))
        self.image_download_path = os.getenv("IMAGE_DOWNLOAD_PATH", os.path.join(os.getcwd(), "images"))

        # Ensure the image download directory exists
        os.makedirs(self.image_download_path, exist_ok=True)


class DatabaseConnection:
    """Handles database connections and queries."""

    def __init__(self, config: ConfigLoader):
        self.conn_str = (
            f'DRIVER={{SQL Server}};'
            f'SERVER={config.db_server};'
            f'DATABASE={config.db_database};'
            f'UID={config.db_username};'
            f'PWD={config.db_password};'
        )
        self.connection = pyodbc.connect(self.conn_str)
        self.cursor = self.connection.cursor()

    def fetch_data(self, query: str):
        logging.info("Fetching data from database...")
        self.cursor.execute(query)
        return pd.DataFrame.from_records(self.cursor.fetchall(), columns=[desc[0] for desc in self.cursor.description])

    def close(self):
        self.cursor.close()
        self.connection.close()
        logging.info("Database connection closed.")


class FileUploader:
    """Handles the file upload process asynchronously."""

    def __init__(self, config: ConfigLoader):
        self.upload_url = config.upload_url
        self.api_key = config.api_key
        self.request_delay = config.request_delay
        self.image_download_path = config.image_download_path
        self.max_retries = 3  # Maximum retry attempts for failed uploads

    def download_image(self, url, product_id, media_id):
        """Downloads the image from the provided URL and saves it locally with a unique filename."""
        original_filename = os.path.basename(urlparse(url).path)
        new_filename = f"{product_id}_{media_id}_{original_filename}"
        file_path = os.path.join(self.image_download_path, new_filename)

        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                with open(file_path, 'wb') as file:
                    for chunk in response.iter_content(1024):
                        file.write(chunk)
                logging.info(f"Image downloaded and saved as: {file_path}")
                return file_path, new_filename
            else:
                logging.error(f"Failed to download image: {url}, Status Code: {response.status_code}")
                return None, None
        except Exception as e:
            logging.error(f"Exception while downloading image {url}: {str(e)}")
            return None, None

    async def upload_file(self, session: aiohttp.ClientSession, row):
        logging.info(
            f"Downloading image from URL: {row['URL']} for Product ID: {row['ProductId']}, Media ID: {row['MediaId']}")
        file_path, file_name = self.download_image(row['URL'], row['ProductId'], row['MediaId'])

        if not file_path:
            return "Error", "Failed to download image", ""

        current_date = datetime.datetime.now(datetime.UTC).isoformat()

        data = {
            "TenantId": "2",
            "EntityType": "product",
            "EntityId": row["ProductId"],
            "MediaResourceId": row["MediaResourceId"],
            "Order": row["Order"],
            "InternalCode": row["ProductId"],
            "MediaType": "image",
            "Date": current_date
        }

        form = aiohttp.FormData()
        with open(file_path, "rb") as file:
            form.add_field("FormFile", file.read(), filename=file_name, content_type=row["ContentType"])
        for key, value in data.items():
            form.add_field(key, str(value))

        headers = {
            "Accept": "application/json",
            "api-key": self.api_key
        }

        for attempt in range(self.max_retries):
            try:
                async with session.post(self.upload_url, headers=headers, data=form, timeout=10) as response:
                    response_text = await response.text()
                    if response.status == 200:
                        logging.info(
                            f"Upload successful for Product ID: {row['ProductId']}, Media ID: {row['MediaId']}")
                        return "Success", "Uploaded successfully", response_text
                    else:
                        logging.error(
                            f"Upload failed (Attempt {attempt + 1}/{self.max_retries}) for Product ID: {row['ProductId']}, Media ID: {row['MediaId']}. Response: {response_text}")
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
            except asyncio.TimeoutError:
                logging.error(
                    f"TimeoutError (Attempt {attempt + 1}/{self.max_retries}) for Product ID: {row['ProductId']}, Media ID: {row['MediaId']}")
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logging.exception(
                    f"Exception occurred (Attempt {attempt + 1}/{self.max_retries}) during upload for Product ID: {row['ProductId']}, Media ID: {row['MediaId']}")
                await asyncio.sleep(2 ** attempt)
        return "Error", "Max retries reached", ""


class ProcessManager:
    """Coordinates the entire process flow asynchronously."""

    def __init__(self):
        self.config = ConfigLoader()
        self.db = DatabaseConnection(self.config)
        self.uploader = FileUploader(self.config)

    async def run(self):
        logging.info("Starting process...")
        query = """
                SELECT TOP 100  p.ProductId, p.sku, p.SystemCode, pm.LinkId,pm.[Order],m.MediaId, m.URL, m.ParentId, m.ImageType, m.MediaResourceId,
                m.CompanyId, m.FileUrlBase64, m.[ContentType], p.Status [productstatus], pm.Status [linkproductmediastatus], m.Status [mediastatus]
                 from mdl04.tblProduct p
                inner JOIN mdl04.linkProductMedia pm ON p.ProductId = pm.ProductId
                INNER JOIN  mdl04.tblMedia m ON pm.MediaId = m.MediaId
                WHERE m.MediaResourceId = '68920485-d222-4ff0-b947-e0340d77b56a' AND p.Status=1 AND pm.Status=1 AND m.Status=1 AND m.ParentId is NULL
                order BY p.createddate ASC
        """

        df = self.db.fetch_data(query)
        total_records = len(df)
        async with aiohttp.ClientSession() as session:
            tasks = [self.uploader.upload_file(session, row) for _, row in df.iterrows()]
            results = await asyncio.gather(*tasks)

        logging.info(f"Total records processed: {total_records}")
        df.to_excel("output.xlsx", index=False, engine="openpyxl")

        self.cleanup()
        logging.info("Process completed successfully. Results saved in output.xlsx")

    def cleanup(self):
        self.db.close()


if __name__ == "__main__":
    asyncio.run(ProcessManager().run())
import asyncio
import datetime
import logging
import os
from urllib.parse import urlparse

import aiohttp
import pandas as pd
import pyodbc
import requests
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("process.log"),  # Log file
        logging.StreamHandler()  # Console output
    ]
)


class ConfigLoader:
    """Handles loading configuration variables from environment files."""

    def __init__(self):
        load_dotenv()
        self.db_server = os.getenv("DB_SERVER")
        self.db_database = os.getenv("DB_DATABASE")
        self.db_username = os.getenv("DB_USERNAME")
        self.db_password = os.getenv("DB_PASSWORD")
        self.upload_url = os.getenv("UPLOAD_URL")
        self.api_key = os.getenv("API_KEY")
        self.request_delay = float(os.getenv("REQUEST_DELAY", 1.0))
        self.image_download_path = os.getenv("IMAGE_DOWNLOAD_PATH", os.path.join(os.getcwd(), "images"))

        # Ensure the image download directory exists
        os.makedirs(self.image_download_path, exist_ok=True)


class DatabaseConnection:
    """Handles database connections and queries."""

    def __init__(self, config: ConfigLoader):
        self.conn_str = (
            f'DRIVER={{SQL Server}};'
            f'SERVER={config.db_server};'
            f'DATABASE={config.db_database};'
            f'UID={config.db_username};'
            f'PWD={config.db_password};'
        )
        self.connection = pyodbc.connect(self.conn_str)
        self.cursor = self.connection.cursor()

    def fetch_data(self, query: str):
        logging.info("Fetching data from database...")
        self.cursor.execute(query)
        return pd.DataFrame.from_records(self.cursor.fetchall(), columns=[desc[0] for desc in self.cursor.description])

    def close(self):
        self.cursor.close()
        self.connection.close()
        logging.info("Database connection closed.")


class FileUploader:
    """Handles the file upload process asynchronously."""

    def __init__(self, config: ConfigLoader):
        self.upload_url = config.upload_url
        self.api_key = config.api_key
        self.request_delay = config.request_delay
        self.image_download_path = config.image_download_path
        self.max_retries = 3  # Maximum retry attempts for failed uploads

    def download_image(self, url, product_id, media_id):
        """Downloads the image from the provided URL and saves it locally with a unique filename."""
        original_filename = os.path.basename(urlparse(url).path)
        new_filename = f"{product_id}_{media_id}_{original_filename}"
        file_path = os.path.join(self.image_download_path, new_filename)

        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                with open(file_path, 'wb') as file:
                    for chunk in response.iter_content(1024):
                        file.write(chunk)
                logging.info(f"Image downloaded and saved as: {file_path}")
                return file_path, new_filename
            else:
                logging.error(f"Failed to download image: {url}, Status Code: {response.status_code}")
                return None, None
        except Exception as e:
            logging.error(f"Exception while downloading image {url}: {str(e)}")
            return None, None

    async def upload_file(self, session: aiohttp.ClientSession, row):
        logging.info(
            f"Downloading image from URL: {row['URL']} for Product ID: {row['ProductId']}, Media ID: {row['MediaId']}")
        file_path, file_name = self.download_image(row['URL'], row['ProductId'], row['MediaId'])

        if not file_path:
            return "Error", "Failed to download image", ""

        current_date = datetime.datetime.now(datetime.UTC).isoformat()

        data = {
            "TenantId": "2",
            "EntityType": "product",
            "EntityId": row["ProductId"],
            "MediaResourceId": row["MediaResourceId"],
            "Order": row["Order"],
            "InternalCode": row["ProductId"],
            "MediaType": "image",
            "Date": current_date
        }

        form = aiohttp.FormData()
        with open(file_path, "rb") as file:
            form.add_field("FormFile", file.read(), filename=file_name, content_type=row["ContentType"])
        for key, value in data.items():
            form.add_field(key, str(value))

        headers = {
            "Accept": "application/json",
            "api-key": self.api_key
        }

        for attempt in range(self.max_retries):
            try:
                async with session.post(self.upload_url, headers=headers, data=form, timeout=10) as response:
                    response_text = await response.text()
                    if response.status == 200:
                        logging.info(
                            f"Upload successful for Product ID: {row['ProductId']}, Media ID: {row['MediaId']}")
                        return "Success", "Uploaded successfully", response_text
                    else:
                        logging.error(
                            f"Upload failed (Attempt {attempt + 1}/{self.max_retries}) for Product ID: {row['ProductId']}, Media ID: {row['MediaId']}. Response: {response_text}")
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
            except asyncio.TimeoutError:
                logging.error(
                    f"TimeoutError (Attempt {attempt + 1}/{self.max_retries}) for Product ID: {row['ProductId']}, Media ID: {row['MediaId']}")
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logging.exception(
                    f"Exception occurred (Attempt {attempt + 1}/{self.max_retries}) during upload for Product ID: {row['ProductId']}, Media ID: {row['MediaId']}")
                await asyncio.sleep(2 ** attempt)
        return "Error", "Max retries reached", ""


class ProcessManager:
    """Coordinates the entire process flow asynchronously."""

    def __init__(self):
        self.config = ConfigLoader()
        self.db = DatabaseConnection(self.config)
        self.uploader = FileUploader(self.config)

    async def run(self):
        logging.info("Starting process...")
        query = """
                SELECT TOP 100  p.ProductId, p.sku, p.SystemCode, pm.LinkId,pm.[Order],m.MediaId, m.URL, m.ParentId, m.ImageType, m.MediaResourceId,
                m.CompanyId, m.FileUrlBase64, m.[ContentType], p.Status [productstatus], pm.Status [linkproductmediastatus], m.Status [mediastatus]
                 from mdl04.tblProduct p
                inner JOIN mdl04.linkProductMedia pm ON p.ProductId = pm.ProductId
                INNER JOIN  mdl04.tblMedia m ON pm.MediaId = m.MediaId
                WHERE m.MediaResourceId = '68920485-d222-4ff0-b947-e0340d77b56a' AND p.Status=1 AND pm.Status=1 AND m.Status=1 AND m.ParentId is NULL
                order BY p.createddate ASC
        """

        df = self.db.fetch_data(query)
        total_records = len(df)
        async with aiohttp.ClientSession() as session:
            tasks = [self.uploader.upload_file(session, row) for _, row in df.iterrows()]
            results = await asyncio.gather(*tasks)

        logging.info(f"Total records processed: {total_records}")
        df.to_excel("output.xlsx", index=False, engine="openpyxl")

        self.cleanup()
        logging.info("Process completed successfully. Results saved in output.xlsx")

    def cleanup(self):
        self.db.close()


if __name__ == "__main__":
    asyncio.run(ProcessManager().run())

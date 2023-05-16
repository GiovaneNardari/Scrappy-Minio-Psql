from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

load_dotenv()

from scrapy.crawler import CrawlerProcess
from spiders.ml_keyboards_spider import MlKeyboardsSpider

from datetime import datetime
import time

def uploadObject(client, bucket: str, newObjectName: str, filePath: str):
    #create bucket if it doesnt already exist
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
        print("Bucket creation done!")
    client.fput_object(bucket, newObjectName, filePath)
    print(f"Upload of {newObjectName} to {bucket} is done!")


def run_spider():
    from scrapy.settings import Settings
    import os

    settings = Settings()
    os.environ['SCRAPY_SETTINGS_MODULE'] = 'settings'
    settings_module_path = os.environ['SCRAPY_SETTINGS_MODULE']
    settings.setmodule(settings_module_path, priority='project')

    process = CrawlerProcess(settings)
    process.crawl(MlKeyboardsSpider)
    process.start()

def main():
    start = time.time()
    # Create client with access key and secret key.
    client_connection = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    run_spider()
    
    current_dt = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
    
    #uploading changes
    uploadObject(client_connection, 
            "aularedes", 
            "KeyboardsPrices"+"("+current_dt+")", 
            "ml_keyboards/results.csv")
    end = time.time()
    print(f"Elapsed time: {end - start}")

if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("An error occurred", exc)

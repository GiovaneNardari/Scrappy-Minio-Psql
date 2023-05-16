from minio import Minio
from minio.error import S3Error

import psycopg2
from datetime import datetime
import subprocess
import time

def downloadObject(client, bucket: str, objectName: str, newFilePath):
    found = client.bucket_exists(bucket)
    if found:
        try:
            client.fget_object(bucket, objectName, newFilePath)
        except:
            print(f'Currently, the object {objectName} does not exists inside of {bucket}.')
    else:
        print("Bucket not found")
    print(f"Download of {objectName} from {bucket} is done!")

def main():
    current_day = datetime.now().strftime("%d-%m-%Y")

    subprocess.Popen(["sh", "open_minio.sh"])

    # Create client with access key and secret key.
    client_connection = Minio(
        "127.0.0.1:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    #verifica os arquivos que foram enviados ao bucket no dia atual (espera-se que sejam ate 24 arquivos, uma vez que a coleta ocorre de hora em hora)
    objects = [obj for obj in client_connection.list_objects(bucket_name = "aularedes", prefix = "KeyboardsPrices(" + current_day + "_")]

    #baixa os arquivos encontrados
    for obj in objects:
        downloadObject(client_connection, "aularedes", obj.object_name, f"downloadedObjects/{obj.object_name}.csv")

    subprocess.Popen(["sh", "close_minio.sh"])

    #iniciando postgreSQL
    subprocess.Popen(["sh", "open_psql.sh"])
    time.sleep(10)

    #conexao com o postgreSQL
    db = psycopg2.connect(database="giovane",
                            host="localhost",
                            user="giovane",
                            password="12341234",
                            port="5432")

    db_cursor = db.cursor() #execute, fetchone, fetchall, fetchmany

    #cria a tabela se ja nao existe
    #campos: 'title','price', 'date', 'time', 'link'
    db_cursor.execute("CREATE TABLE IF NOT EXISTS tbl_keyboard_prices (title varchar(200) NOT NULL, price varchar(50) NOT NULL, date varchar(50) NOT NULL, time varchar(50) NOT NULL, link varchar(2500) NOT NULL);")

    #baixa todos os arquivos referentes ao dia atual encontrados
    for obj in objects:
        with open(f"downloadedObjects/{obj.object_name}.csv", "r") as f:
            data = []
            for line in f.readlines()[1:]:
                line_data = line.split(",")
                data.append(tuple((line_data[0], line_data[1], line_data[2], line_data[3], line_data[4])))
            #upload do conteudo dos arquivos para o banco
            db_cursor.executemany("INSERT INTO tbl_keyboard_prices(title, price, date, time, link) VALUES(%s, %s, %s, %s, %s)", data)

    db.commit()
    db_cursor.close()
    db.close()

    time.sleep(10)
    subprocess.Popen(["sh", "close_psql.sh"])
    print("Done!")

if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("An error occurred", exc)
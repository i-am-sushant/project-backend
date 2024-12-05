from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import psycopg2
import os
from dotenv import load_dotenv
from utils.storage import (
    upload_file_to_blob, 
    download_file_from_blob,
    delete_file_from_blob,
    list_blobs_in_container
    )
from io import BytesIO

load_dotenv()

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

db_url = os.getenv('POSTGRES_URL')

def get_db_connection():
    conn = psycopg2.connect(db_url)
    return conn

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files['file']
    file_content = file.read()
    file_url = upload_file_to_blob(file.filename, file_content)

    if not file_url:
        return jsonify({"error": "File upload failed"}), 500
    
    file_size_kb = len(file_content) / 1024

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO file_metadata (file_name, file_type, file_size, blob_url)
        VALUES (%s, %s, %s, %s)
        RETURNING id;
    """, (file.filename, file.content_type, file_size_kb, file_url))
    file_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({"id": file_id, "file_url": file_url}), 200

@app.route('/files', methods=['GET'])
def list_files():
    files = list_blobs_in_container()
    return jsonify(files), 200

@app.route('/download/<path:file_name>', methods=['GET'])
def download_file(file_name):
    file_content = download_file_from_blob(file_name)
    if not file_content:
        return jsonify({"error": "File not found"}), 404

    return send_file(BytesIO(file_content), 
                     download_name=file_name, 
                     as_attachment=True)

@app.route('/delete/<string:file_name>', methods=['DELETE'])
def delete_file(file_name):
    delete_file_from_blob(file_name)
    # Optionally, delete from metadata table if you're maintaining it
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute("DELETE FROM file_metadata WHERE file_name = %s", (file_name,))
    connection.commit()
    cursor.close()
    return jsonify({'message': 'File deleted successfully'}), 200

if __name__ == '__main__':
    app.run(debug=True)

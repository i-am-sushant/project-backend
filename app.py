from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import os
from dotenv import load_dotenv
from utils.storage import upload_file_to_blob

load_dotenv()

app = Flask(__name__)
CORS(app)

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
    file_url = upload_file_to_blob(file.filename, file.read())

    if not file_url:
        return jsonify({"error": "File upload failed"}), 500

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO file_metadata (file_name, file_type, file_size, blob_url)
        VALUES (%s, %s, %s, %s)
        RETURNING id;
    """, (file.filename, file.content_type, len(file_content) / (1024 * 1024), file_url))
    file_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({"id": file_id, "file_url": file_url})

if __name__ == '__main__':
    app.run(debug=True)

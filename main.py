from google.cloud import bigquery
from flask import Flask, request, jsonify
import json
import os

app = Flask(__name__)

@app.route('/', methods=['POST'])
def ingest_transaction():
    client = bigquery.Client()
    table_id = "fraud-detector-453909.dataset.transactions"

    try:
        envelope = request.get_json()
        if not envelope:
            return jsonify({"error": "No Pub/Sub message received"}), 400

        if not isinstance(envelope, dict) or "message" not in envelope or "data" not in envelope["message"]:
             return jsonify({"error": "Invalid Pub/Sub message format"}), 400

        pubsub_message = envelope["message"]
        message_data = pubsub_message["data"]
        decoded_data = json.loads(base64.b64decode(message_data).decode('utf-8'))

        rows_to_insert = [decoded_data]
        errors = client.insert_rows_json(table_id, rows_to_insert)

        if errors:
            return jsonify({"error": f"Error inserting rows: {errors}"}), 500
        return jsonify({"message": "Transaction ingested successfully"}), 200

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

import base64
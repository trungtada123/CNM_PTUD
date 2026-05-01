from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import os

app = Flask(__name__)
# Enable CORS so the future frontend can communicate with this backend easily
CORS(app)

# The URL of your BentoML prediction service (typically runs on port 3000)
BENTOML_SERVICE_URL = os.getenv("BENTOML_URL", "http://localhost:3000/predict")


@app.route('/')
def home():
    return jsonify({"status": "success", "message": "Flask backend is running successfully!"})


@app.route('/api/predict', methods=['POST'])
def predict():
    """
    Endpoint that accepts data from the frontend and forwards it to the BentoML service.
    """
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400

        # Send request to BentoML churn_serving service
        response = requests.post(BENTOML_SERVICE_URL, json=data)
        response.raise_for_status()
        
        return jsonify(response.json())
    except requests.exceptions.RequestException as e:
        return jsonify({
            "error": "Failed to connect to the prediction service (BentoML)",
            "details": str(e)
        }), 500

if __name__ == '__main__':
    # Run the Flask app on port 5050
    app.run(host='0.0.0.0', port=5050, debug=True)

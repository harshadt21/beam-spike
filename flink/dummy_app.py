import time
import logging
from flask import Flask, request, jsonify
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address


app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["10 per minute", "1 per second"],
    storage_uri="memory://",
)


@app.route('/api/data', methods=['POST'])
@limiter.limit("45 per second", override_defaults=True)
def process_data():
    """
    Accepts a JSON payload, logs it, and returns a response after a delay.
    """
    # Simulate a network delay of 200 milliseconds (0.2 seconds).
    delay_ms = 100
    time.sleep(delay_ms / 1000)

    try:
        # Check if the request body is valid JSON
        if not request.is_json:
            logging.warning("Received a non-JSON request.")
            return jsonify({"error": "Request must be JSON"}), 400

        # Get the JSON payload from the request
        payload = request.get_json()

        # Log the received payload.
        # The logging message includes the remote IP and the received data.
        logging.info(f"Received payload from {get_remote_address()}: {payload}")

        # Construct the success response
        response = {
            "status": "success",
            "message": f"Payload received and processed after {delay_ms}ms delay.",
            "data_received": payload
        }

        # Return the JSON response with a 200 OK status code
        return jsonify(response), 200

    except Exception as e:
        # Catch any other potential errors and return a 500 status code
        logging.error(f"An error occurred: {e}")
        return jsonify({"error": "Internal Server Error"}), 500

# The main block to run the Flask application.
# `debug=True` is useful for development as it provides more detailed
# error messages and hot-reloads the server on code changes.
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

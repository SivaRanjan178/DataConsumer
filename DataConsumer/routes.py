from flask import request, jsonify
from redis_service import RedisService
import traceback

def init_routes(app):
    @app.route('/api/redis/save', methods=['POST'])
    def save_data():
        try:
            data = request.get_json()
            key = data.get('key')
            value = data.get('value')
            if not key or not value:
                return jsonify({"error": "Key and value are required"}), 400
            RedisService.save_data(key, value)
            return jsonify({"message": "Data saved successfully!"})
        except Exception as e:
            print(f"Error: {e}")
            print(traceback.format_exc())
            return jsonify({"error": "Internal server error"}), 500

    @app.route('/api/redis/get', methods=['GET'])
    def get_data():
        try:
            key = request.args.get('key')
            if not key:
                return jsonify({"error": "Key is required"}), 400
            data = RedisService.get_data(key)
            if data is None:
                return jsonify({"message": "Data not found"}), 404
            return jsonify({"data": data.decode('utf-8')})
        except Exception as e:
            print(f"Error: {e}")
            print(traceback.format_exc())
            return jsonify({"error": "Internal server error"}), 500

    @app.route('/api/redis/delete', methods=['DELETE'])
    def delete_data():
        try:
            key = request.args.get('key')
            if not key:
                return jsonify({"error": "Key is required"}), 400
            RedisService.delete_data(key)
            return jsonify({"message": "Data deleted successfully!"})
        except Exception as e:
            print(f"Error: {e}")
            print(traceback.format_exc())
            return jsonify({"error": "Internal server error"}), 500
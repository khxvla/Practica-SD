"""REST Server for Direct Communication Architecture"""
import sys
import os
import time
from flask import Flask, request, jsonify
from functools import wraps

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.common.redis_backend import RedisBackend
from src.common.logger import get_logger
from src.common.config import REST_HOST, REST_PORT

logger = get_logger(__name__)
app = Flask(__name__)

# Global Redis backend
redis_backend = None

def init_redis(max_retries=5, delay=1):
    """Initialize Redis backend with retry attempts.

    Tries up to max_retries times with a delay (seconds) between attempts.
    Returns True if initialized, False otherwise.
    """
    global redis_backend
    for attempt in range(1, max_retries + 1):
        try:
            redis_backend = RedisBackend()
            logger.info("Redis backend initialized")
            return True
        except Exception as e:
            logger.warning(f"Redis init attempt {attempt} failed: {e}")
            if attempt < max_retries:
                time.sleep(delay)
            else:
                logger.error("Exceeded Redis init retries")
                return False


def require_redis(f):
    """Decorator to ensure Redis is available. Attempts a quick init if not present."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if redis_backend is None:
            # Try a few quick attempts to initialize Redis before returning 503
            if not init_redis(max_retries=3, delay=1):
                return jsonify({"error": "Redis not available"}), 503
        return f(*args, **kwargs)
    return decorated_function

# ============================================================================
# Health Check
# ============================================================================

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({"status": "ok"}), 200

# ============================================================================
# Unnumbered Tickets
# ============================================================================

@app.route('/api/buy/unnumbered', methods=['POST'])
@require_redis
def buy_unnumbered():
    """
    Buy an unnumbered ticket.
    
    Request body:
    {
        "client_id": "client_001",
        "request_id": 1
    }
    
    Response:
    {
        "success": true/false,
        "message": "..."
    }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Missing request body"}), 400
        
        client_id = data.get("client_id")
        request_id = data.get("request_id")
        
        if not client_id or request_id is None:
            return jsonify({"error": "Missing client_id or request_id"}), 400
        
        # --- SECCIÓN 4 DEL ENUNCIADO: REQUISITO DE REALISMO (DELAY 100ms) ---
        # Modela la latencia externa de procesamiento de pasarelas de pago bancarias
        time.sleep(0.100)
        
        # Attempt purchase
        success = redis_backend.buy_unnumbered_idempotent(client_id, request_id)
        
        response = {
            "success": success,
            "client_id": client_id,
            "request_id": request_id,
            "type": "unnumbered"
        }
        
        if success:
            response["message"] = "Ticket purchased successfully"
            return jsonify(response), 200
        else:
            response["message"] = "All tickets sold out"
            return jsonify(response), 409
    
    except Exception as e:
        logger.error(f"Error in buy_unnumbered: {e}")
        return jsonify({"error": str(e)}), 500

# ============================================================================
# Numbered Tickets
# ============================================================================

@app.route('/api/buy/numbered/<int:seat_id>', methods=['POST'])
@require_redis
def buy_numbered(seat_id):
    """
    Buy a specific numbered seat.
    
    URL: /api/buy/numbered/<seat_id>
    
    Request body:
    {
        "client_id": "client_001",
        "request_id": 1
    }
    
    Response:
    {
        "success": true/false,
        "seat_id": <seat_id>,
        "message": "..."
    }
    """
    try:
        data = request.get_json() if request.is_json else {}
        
        client_id = data.get("client_id")
        request_id = data.get("request_id")
        
        if not client_id or request_id is None:
            return jsonify({"error": "Missing client_id or request_id"}), 400
        
        # Validate seat ID
        if seat_id < 1 or seat_id > 20000:
            return jsonify({
                "error": f"Invalid seat ID: {seat_id}. Must be between 1 and 20000"
            }), 400
        
        # --- SECCIÓN 4 DEL ENUNCIADO: REQUISITO DE REALISMO (DELAY 100ms) ---
        # Modela la latencia externa de procesamiento de pasarelas de pago bancarias
        time.sleep(0.100)
        
        # Attempt purchase
        success = redis_backend.buy_numbered_idempotent(seat_id, client_id, request_id)
        
        response = {
            "success": success,
            "client_id": client_id,
            "request_id": request_id,
            "seat_id": seat_id,
            "type": "numbered"
        }
        
        if success:
            response["message"] = f"Seat {seat_id} purchased successfully"
            return jsonify(response), 200
        else:
            response["message"] = f"Seat {seat_id} already sold"
            return jsonify(response), 409
    
    except Exception as e:
        logger.error(f"Error in buy_numbered: {e}")
        return jsonify({"error": str(e)}), 500

# ============================================================================
# Statistics
# ============================================================================

@app.route('/api/stats', methods=['GET'])
@require_redis
def get_stats():
    """Get current system statistics."""
    try:
        stats = redis_backend.get_stats()
        return jsonify(stats), 200
    except Exception as e:
        logger.error(f"Error in get_stats: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/stats/detailed', methods=['GET'])
@require_redis
def get_detailed_stats():
    """Get detailed statistics including sold seats."""
    try:
        stats = redis_backend.get_stats()
        sold_seats = list(redis_backend.get_sold_seats())[:100]
        
        return jsonify({
            **stats,
            "sample_sold_seats": sold_seats,
            "total_sample_seats": len(sold_seats)
        }), 200
    except Exception as e:
        logger.error(f"Error in get_detailed_stats: {e}")
        return jsonify({"error": str(e)}), 500

# ============================================================================
# Admin Endpoints
# ============================================================================

@app.route('/api/admin/reset', methods=['POST'])
@require_redis
def reset():
    """Reset system (admin only - use carefully)."""
    try:
        redis_backend.clear_all()
        redis_backend.initialize_system()
        return jsonify({"message": "System reset successfully"}), 200
    except Exception as e:
        logger.error(f"Error in reset: {e}")
        return jsonify({"error": str(e)}), 500

# ============================================================================
# Error Handlers
# ============================================================================

@app.errorhandler(404)
def not_found(error):
    """404 handler."""
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    """500 handler."""
    logger.error(f"Internal server error: {error}")
    return jsonify({"error": "Internal server error"}), 500

# ============================================================================
# Main
# ============================================================================

def main(host=REST_HOST, port=REST_PORT, debug=False):
    """Start the REST server."""
    logger.info(f"Starting REST server on {host}:{port}")
    
    if not init_redis():
        logger.error("Failed to initialize Redis")
        sys.exit(1)
    
    logger.info("Redis connection ready")
    if debug:
        logger.info("Starting Flask development server...")
        app.run(host=host, port=port, debug=debug, threaded=True)
        return

    try:
        from waitress import serve

        logger.info("Starting Waitress server...")
        serve(app, host=host, port=port, threads=16)
    except ImportError:
        logger.warning("Waitress not available, falling back to Flask development server")
        app.run(host=host, port=port, debug=debug, threaded=True)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="REST Server for Ticket System")
    parser.add_argument("--host", default=REST_HOST, help="Host to bind to")
    parser.add_argument("--port", type=int, default=REST_PORT, help="Port to bind to")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    
    args = parser.parse_args()
    
    main(host=args.host, port=args.port, debug=args.debug)

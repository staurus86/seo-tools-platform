"""
Minimal test server - проверка базового запуска
"""
import os
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
import logging

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

PORT = int(os.environ.get('PORT', '8000'))

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(b'{"status": "ok", "path": "%s"}' % self.path.encode())
    
    def log_message(self, format, *args):
        logger.info("%s - %s" % (self.address_string(), format % args))

if __name__ == '__main__':
    logger.info(f"Starting minimal server on port {PORT}")
    server = HTTPServer(('0.0.0.0', PORT), Handler)
    logger.info(f"Server running at http://0.0.0.0:{PORT}")
    server.serve_forever()

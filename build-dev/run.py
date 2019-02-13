import logging
import http.server
import SocketServer
import getpass

class MyHTTPHandler(http.server.SimpleHTTPRequestHandler):
   def log_message(self, format, *args):
       logging.info("%s - - [%s] %s\n"% (
           self.client_address[0],
           self.log_date_time_string(),
           format%args
       ))

logging.basicConfig(
   filename='/log/http-server.log',
   format='%(asctime)s - %s(levelname)s - %(message)s',
   level=logging.INFO
)
logging.getLogger().addHandler(logging.StreamHandler())
logging.info('inicializando ...')
PORT = 8000

httpd = SocketServer.TCPServer(("", PORT), MyHTTPHandler)
logging.info('escutando a porta: %s', PORT)
logging.info('usuario: %s', getpass.getuser())
httpd.serve_forever()

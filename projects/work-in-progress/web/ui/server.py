# Extended python -m http.serve with --username and --password parameters for
# basic auth, based on https://gist.github.com/fxsjy/5465353

from functools import partial
from http.server import SimpleHTTPRequestHandler, test
import base64
import os

# class to handle http basic authentication
class AuthHTTPRequestHandler(SimpleHTTPRequestHandler):
    """ Main class to present webpages and authentication. """

    def __init__(self, *args, **kwargs):
        username = os.environ["DEMO_USER"]
        password = os.environ["DEMO_PASS"]
        self._auth = base64.b64encode(f"{username}:{password}".encode()).decode()
        super().__init__(*args, **kwargs)

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()

    def do_AUTHHEAD(self):
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="Test"')
        self.send_header("Content-type", "text/html")
        self.end_headers()

    def do_GET(self):
        """ Present frontpage with user authentication. """
        if self.headers.get("Authorization") == None:
            self.do_AUTHHEAD()
            self.wfile.write(b"no auth header received")
        elif self.headers.get("Authorization") == "Basic " + self._auth:
            SimpleHTTPRequestHandler.do_GET(self)
        else:
            self.do_AUTHHEAD()
            self.wfile.write(self.headers.get("Authorization").encode())
            self.wfile.write(b"not authenticated")

# main
if __name__ == "__main__":
    import argparse

    # read command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--cgi", action="store_true", help="Run as CGI Server")
    parser.add_argument(
        "--bind",
        "-b",
        metavar="ADDRESS",
        default="127.0.0.1",
        help="Specify alternate bind address " "[default: all interfaces]",
    )
    parser.add_argument(
        "--directory",
        "-d",
        default=os.getcwd(),
        help="Specify alternative directory " "[default:current directory]",
    )
    parser.add_argument(
        "port",
        action="store",
        default=9002,
        type=int,
        nargs="?",
        help="Specify alternate port [default: 8000]",
    )

    # parse arguments
    args = parser.parse_args()

    # instantiate handler
    handler_class = partial(
        AuthHTTPRequestHandler,
        directory=args.directory,
    )

    # call handler
    test(HandlerClass = handler_class, port = args.port, bind = args.bind)


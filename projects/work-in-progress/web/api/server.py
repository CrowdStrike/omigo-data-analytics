from fastapi import BackgroundTasks
from fastapi import Depends, FastAPI, HTTPException, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from omigo_core import s3io_wrapper
from omigo_core import tsv, utils, funclib
from omigo_ext import graphviz_ext
import json
import os
import secrets
import uvicorn

# This sample app is based on the tutorial
# https://fastapi.tiangolo.com/tutorial/

# initialize app
app = FastAPI()

# set the origins for CORS
origins = [
    # "http://127.0.0.1", # These are not needed
    # "http://127.0.0.1:9001", # These are not needed
    "http://127.0.0.1:9002"
]

# add middleware to handle CORS headers
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# list of headers that are returned in response
standard_headers = {
    "Access-Control-Allow-Origin": "*",
    "Content-Type": "application/json"
}

# Add a basic HTTP authentication
security = HTTPBasic()

# method to validate credentials using HTTP Basic Auth
def validate_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    # encode the credentials to compare
    input_user_name = credentials.username.encode("utf-8")
    input_password = credentials.password.encode("utf-8")

    # DO NOT STORE passwords in plain text. Store them in secure location like vaults or database after encryption.
    # This is just shown for educational purposes
    stored_username = bytes(os.environ["DEMO_USER"], "utf-8")
    stored_password = bytes(os.environ["DEMO_PASS"], "utf-8")

    # compare the username and password with expected input
    is_username = secrets.compare_digest(input_user_name, stored_username)
    is_password = secrets.compare_digest(input_password, stored_password)

    # check for validity
    if is_username and is_password:
        return { "status": "Authorized", "username": credentials.username } 

    # raise exception if no successful login found
    raise HTTPException(status_code = status.HTTP_401_UNAUTHORIZED,
                        detail = "Invalid credentials",
                        headers = { "WWW-Authenticate": "Basic" })

@app.get("/")
async def root(request: Request):
    content = {"result": {"message": "App Running"}}
    return JSONResponse(content = content, headers = standard_headers)

@app.get("/hello")
async def do_hello(request: Request, auth: str = Depends(validate_credentials)):
    content = {"result": {"message": "hello", "authentication": auth }}
    return JSONResponse(content = content, headers = standard_headers)

@app.get("/get_iris_data")
async def get_iris_data(request: Request, auth: str = Depends(validate_credentials)):
    xtsv = tsv.read("https://raw.githubusercontent.com/CrowdStrike/omigo-data-analytics/main/data/iris.tsv")
    content = { "data": xtsv.add_seq_num("sno").to_maps() }
    return JSONResponse(content = content, headers = standard_headers)

@app.get("/get_avengers")
async def get_avengers(request: Request, node_props: str = None, edge_props: str = None, output_format: str = None, auth: str = Depends(validate_credentials)):
    # read data
    vtsv = tsv.read("marvel_vtsv.tsv")
    etsv = tsv.read("marvel_etsv.tsv")

    # convert to desired output format and return
    node_props = utils.split_str_to_arr(utils.resolve_default_parameter("node_props", node_props, "", "demo"))
    edge_props = utils.split_str_to_arr(utils.resolve_default_parameter("edge_props", edge_props, "", "demo"))
    output_format = utils.resolve_default_parameter("output_format", output_format, "graphviz", "demo")

    # switch
    content = None
    if (output_format == "graphviz"):
        # define custom function to control the style
        def custom_style_func(mp):
            props = graphviz_ext.__default_dot_style_func__(mp)
            if (mp["type"] == "actor"):
                props["shape"] = "oval"
                props["color"] = "lightgreen"
            else:
                props["shape"] = "rectangle"
                props["color"] = "lightgrey"        
            return props    

        # generate the graph string
        digraph_str = graphviz_ext.get_graphviz_data(vtsv, etsv, "name", "src", "dst", vertex_display_id_col = "name", node_props = node_props, edge_props = edge_props,
            style_func = custom_style_func, max_len = 50, display_vertex_keys = ["type"], display_edge_keys = ["type"])
        content = { "digraph": digraph_str }
    else:
        # get the standard nodes and links format
        nodes = vtsv.drop_cols("id").rename("name", "id").to_maps()
        links = etsv.noop("type", "acts_in").rename("src", "source").rename("dst", "target").add_const("value", "1").to_maps()
        content = { "nodes": nodes, "links": links }

    # return 
    return JSONResponse(content = content, headers = standard_headers)

# main
if __name__ == "__main__":
    # run the app only on localhost
    uvicorn.run(app, host = "localhost", port = 9001)

    # run the app on all network interfaces
    # uvicorn.run(app, host = "*", port = 9001)


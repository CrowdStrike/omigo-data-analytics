from fastapi import BackgroundTasks
from fastapi import Depends, FastAPI, HTTPException, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from omigo_core import s3io_wrapper
from omigo_core import tsv, utils, funclib
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
    "http://localhost"
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

# main
if __name__ == "__main__":
    # run the app only on localhost
    uvicorn.run(app, host = "localhost", port = 9001)

    # run the app on all network interfaces
    # uvicorn.run(app, host = "*", port = 9001)


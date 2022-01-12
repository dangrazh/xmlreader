import sys
from os import path
import pathlib

from flask import Flask
from webapp.appconfig import AppConfig

# set the max recursion level to 2'500 (default is 1'000) for more complex xml files
# to be serialized as part of the session handling with flask-kvsession
sys.setrecursionlimit(9500)


# Flask specific part
app = Flask(__name__)
app.config["SECRET_KEY"] = "this should be my secret"


# SQLAlchemy specific part
# from flask_sqlalchemy import SQLAlchemy

# app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///site.db"
# db = SQLAlchemy(app)


# flask debugger part
# from flask_debugtoolbar import DebugToolbarExtension

# app.debug = True
# toolbar = DebugToolbarExtension(app)


# Session handling specific part - flask-kvsession
from flask_kvsession import KVSessionExtension
from simplekv.fs import FilesystemStore

store = FilesystemStore("./webapp/sessiondata")
KVSessionExtension(store, app)


# App configuration part
APP_PATH = str(pathlib.Path(__file__).parent.absolute())
config = AppConfig(APP_PATH + "/config.yaml")
app_config = config.get_full_config()

APP_TITLE = app_config["Application"]["Title"]
APP_VERSION = app_config["Application"]["Version"]
APP_AUTHOR = app_config["Application"]["Developer"]

from webapp import routes
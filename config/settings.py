"""Configuration file"""

import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "database": os.getenv("DB_NAME"),
}

LOGS_FILE = os.getenv("LOGS_FILE")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

DNCP_BASE_URL = os.getenv("DNCP_BASE_URL")

BASE_OUTPUT_DIR = os.getenv("BASE_OUTPUT_DIR")

BASE_LOGS_DIR = os.getenv("BASE_LOGS_DIR")

BASE_OUTPUT_RAW_DIR = os.getenv("BASE_OUTPUT_RAW_DIR")

BASE_OUTPUT_PROCESSED_DIR = os.getenv("BASE_OUTPUT_PROCESSED_DIR")

BASE_INPUT_EXTERNAL_DATA_DIR = os.getenv("BASE_INPUT_EXTERNAL_DATA_DIR")

LLM_ENDPOINT = os.getenv("LLM_ENDPOINT")

LLM_USERNAME = os.getenv("LLM_USERNAME")

LLM_PASSWORD = os.getenv("LLM_PASSWORD")

OPENROUTER_API_KEY = os.getenv("OPEN_ROUTER_API_KEY")

DEFAULT_BATCH_SIZE = os.getenv("DEFAULT_BATCH_SIZE")

DEFAULT_MAX_RETRIES = os.getenv("DEFAULT_MAX_RETRIES")

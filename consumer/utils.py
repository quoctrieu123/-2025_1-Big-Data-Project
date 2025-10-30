import os
from dotenv import load_dotenv

def load_environment_variables(env_file_path=".env"): 
    if os.path.exists(env_file_path):
        load_dotenv(env_file_path)

    env_vars = {}
    for key, value in os.environ.items():
        env_vars[key] = value

    return env_vars
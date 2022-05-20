# import json
# import os

# from flask import Flask, jsonify
# from flask_cors import CORS
# from OpenSSL import SSL

# current_directory = os.path.dirname(os.path.abspath(__file__))
# context = SSL.Context(SSL.SSLv23_METHOD)
# config = json.load(open(os.path.join(current_directory, "config.json")))
# cert = os.path.join(os.path.dirname(__file__), config["SERVER_CER"])
# key = os.path.join(os.path.dirname(__file__), config["SERVER_KEY"])
# context = (cert, key)
# app = Flask(__name__)
# CORS(app)
# app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024


# def health():
#     return jsonify(
#         {
#             "endpoint": f"""https:{config["HOSTNAME"]}:{config["PORT"]}""",
#             "routes": [i.endpoint for i in app.url_map.iter_rules()],
#         }
#     )


# if __name__ == "__main__":
#     import doctest

#     doctest.testmod()

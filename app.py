from flask import Flask, redirect, request, jsonify
import requests
from oauth_flask.utils import verify_response
from urllib.parse import urlencode
from oauth_flask.keys import CLIENT_ID, CLIENT_SECRET, BASE_URL
from oauth_flask.sqlite_db import SQLiteDB

app = Flask(__name__)
db = SQLiteDB()


@app.route("/initiate")
def initiate_auth():
    app_config = {"clientId": CLIENT_ID, "baseUrl": BASE_URL}

    options = {
        "requestType": "code",
        "redirectUri": "http://localhost:3000/oauth/callback",
        "clientId": app_config["clientId"],
        "scopes": ["contacts.readonly"],
    }

    params = {
        "response_type": options["requestType"],
        "redirect_uri": options["redirectUri"],
        "client_id": options["clientId"],
        "scope": " ".join(options["scopes"]),
    }

    authorize_url = f"{app_config['baseUrl']}/oauth/chooselocation?{urlencode(params)}"
    return redirect(authorize_url)


@app.route("/oauth/callback")
def handle_callback():
    app_config = {"clientId": CLIENT_ID, "clientSecret": CLIENT_SECRET}

    data = {
        "client_id": app_config["clientId"],
        "client_secret": app_config["clientSecret"],
        "grant_type": "authorization_code",
        "code": request.args.get("code"),
        "user_type": "Location",
        "redirect_uri": "http://localhost:3000/oauth/callback",
    }

    headers = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post("https://services.leadconnectorhq.com/oauth/token", data=data, headers=headers)

    if verify_response(response.json()):
        db.insert_or_update_token(response.json())
        # return to initiate
        return redirect("/initiate")

    return jsonify({"status": "error"})


if __name__ == "__main__":
    app.run(port=3000)

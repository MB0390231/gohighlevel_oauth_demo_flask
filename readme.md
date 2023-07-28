# Readme for Lead Connector OAuth Flask App

This is a Python Flask web application that demonstrates how to use the Lead Connector OAuth 2.0 API to authenticate and obtain access tokens for accessing resources from the Lead Connector API. The application uses SQLite for storing and managing access tokens.

## Getting Started

To run this application, follow the steps below:

### Prerequisites

- Python 3.x
- Flask
- requests

### Installation

1. Clone the repository to your local machine.

2. Install the required dependencies using pip:

```
pip install -r requirements.txt
```

### Configuration

Setting and Saving Environment Variables:

To set an environment variable, you can use the export command on Linux and macOS or the set command on Windows. For example, to set an environment variable named MY_VARIABLE with the value myproject.settings, you can run the following commands:

```
export MY_VARIABLE=myproject.settings
```

The required environment variables are the CLIENT_ID and CLIENT_SECRET, which can be found after creating your highlevel application and generating them.

### Running the Application

To run the Flask application, execute the following command in your terminal:

```
python app.py
```

By default, the application will be accessible at `http://localhost:3000/`.

## Endpoints

### `/initiate`

This endpoint initiates the OAuth 2.0 authorization flow by redirecting the user to the Lead Connector OAuth authorization page.

### `/oauth/callback`

This endpoint handles the callback from the Lead Connector OAuth authorization page. It exchanges the authorization code for an access token and stores it in the SQLite database. If the process is successful, the user will be redirected to the `/initiate` endpoint again.

### `/refresh`

This endpoint is used to refresh an expired access token. It receives the `refresh_token` as a query parameter and sends a request to the Lead Connector token endpoint to get a new access token. The new access token is then stored in the SQLite database.

## SQLite Database

The application uses SQLite to store and manage access tokens. The database is initialized in the `sqlite_db.py` module, and a single instance of the database is shared across the application using a thread-local storage.

## Notes

- This application is intended for demonstration purposes and might not be suitable for production use without further security measures.

- Ensure that you keep your Lead Connector credentials (`CLIENT_ID` and `CLIENT_SECRET`) secure and do not share them publicly.

- Make sure to review the Lead Connector API documentation for more details on how to use the API and the available endpoints.

- For production use, consider implementing additional security measures like using HTTPS, implementing proper error handling, and securing sensitive data.

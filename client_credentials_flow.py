import requests
import base64
import argparse

def create_token(client_path, secret_path):
    client = open(client_path).read().strip()
    secret = open(secret_path).read().strip()

    url = "https://accounts.spotify.com/api/token"
    data = { "grant_type": "client_credentials" }
    r = requests.post(url, data=data, auth=(client, secret)).json()
    print("this access token will expire in {0} minutes".format(r["expires_in"] / 60.0))
    token = r["access_token"]

    return token

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read spotify client and secret")
    parser.add_argument("--client", type=str, help="spotify client code")
    parser.add_argument("--secret", type=str, help="spotify client secret")
    args = parser.parse_args()

    create_token(args.client, args.secret)

"""
$ python client_credentials_flow.py --client /path/to/spotify/client --secret /path/to/spotify/secret
"""

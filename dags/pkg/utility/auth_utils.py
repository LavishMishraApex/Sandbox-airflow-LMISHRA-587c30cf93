import json
from datetime import datetime, timezone

import jwt
import requests


ENVIRONMENT = "dev"


def get_ascend_jwt(
    credentials: dict,
    base_url: str = f"https://afs-api.{ENVIRONMENT}.gcp.apexclearing.com",
) -> str:
    """
    Generates an Authorization JWT for the Ascend API by generating
    a JSON Web Signature (JWS) from a given credentialand sending
    it to the token endpoint.
    :param credentials: A dictionary containing the following keys:
        - name: The name of the service account.
        - privateKey: The private key of the service account.
        - organization: The organization ID of the service account.
        or
        - entity: The entity ID of the service account.
    :param base_url: The base URL of the Ascend API.
    :return: The Authorization JWT.
    """

    if "name" not in credentials:
        raise KeyError("Name is a required field for credentials")
    if not any(key in credentials for key in ["organization", "entity"]):
        raise KeyError("Credentials must have an organization or entity")

    jws_body = {
        "name": credentials["name"],
        "datetime": datetime.now(timezone.utc).isoformat(),
    }

    if "organization" in credentials:
        jws_body["organization"] = credentials["organization"]
    else:
        jws_body["entity"] = credentials["entity"]

    request_payload = {
        "jws": jwt.encode(jws_body, credentials["privateKey"], algorithm="RS256")
    }

    # The Authorization JWT is returned as a JSON object.
    request_headers = {"Accept": "application/json"}

    response = requests.post(
        base_url + "/iam/v1beta/serviceAccounts:generateAccessToken",
        headers=request_headers,
        json=request_payload,
    )

    if response.status_code == 200:
        body = json.loads(response.content)
        return body["access_token"]
    else:
        raise RuntimeError(
            f"Failed to get token, endpoint: {base_url} returned {response.status_code} {response.text}"
        )

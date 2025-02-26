from typing import Dict, List, Optional
from config.config import config
import requests
import time
import os


class PetfinderAPI:
    def __init__(self, filename="config/config.ini", section="petfinder"):
        self.credentials = config(filename=filename, section=section)
        self.access_token = None
        self.token_expiry = 0

    def get_access_token(self):
        data = {
            "grant_type": "client_credentials",
            "client_id": self.credentials["api_key"],
            "client_secret": self.credentials["api_secret"],
        }
        response = requests.post(f"{self.credentials["base_url"]}/oauth2/token", data=data)
        response.raise_for_status()

        self.access_token = response.json()["access_token"]
        self.token_expiry = (
            time.time() + response.json()["expires_in"] - 60
        )  # 60 seconds buffer
        return self.access_token

    def _ensure_valid_token(self) -> None:
        """Ensure we have a valid access token"""
        if not self.access_token or time.time() >= self.token_expiry:
            self.get_access_token()

    def _make_request(
        self, endpoint: str, method: str = "GET", params: Dict = None
    ) -> Dict:
        """Make an authenticated request to the Petfinder API"""
        self._ensure_valid_token()

        headers = {"Authorization": f"Bearer {self.access_token}"}

        url = f"{self.credentials["base_url"]}/{endpoint}"

        response = requests.request(
            method=method, url=url, headers=headers, params=params
        )

        response.raise_for_status()
        return response.json()

    def get_dogs(self, page: int = 1, limit: int = 100, **kwargs) -> Dict:
        """
        Get a list of dogs from Petfinder

        Args:
            page (int): Page number of results
            limit (int): Number of results per page (max 100)
            **kwargs: Additional filter parameters (e.g., location, distance, etc.)
        """
        params = {"type": "Dog", "page": page, "limit": limit, **kwargs}

        return self._make_request("animals", params=params)

    def get_dog_by_id(self, dog_id: int) -> Dict:
        """Get details for a specific dog by ID"""
        return self._make_request(f"animals/{dog_id}")

    def get_breeds(self) -> List[str]:
        """Get a list of all dog breeds"""
        response = self._make_request("types/Dog/breeds")
        return [breed["name"] for breed in response["breeds"]]

    def search_dogs(
        self,
        location: str = None,
        distance: int = None,
        breed: str = None,
        size: str = None,
        gender: str = None,
        age: str = None,
        color: str = None,
        coat: str = None,
        status: str = None,
        page: int = 1,
    ) -> Dict:
        """
        Search for dogs with specific criteria

        Args:
            location (str): City, State or Postal Code
            distance (int): Distance in miles from location
            breed (str): Breed name
            size (str): small, medium, large, xlarge
            gender (str): male, female
            age (str): baby, young, adult, senior
            color (str): Color of the dog
            coat (str): short, medium, long, wire, hairless, curly
            status (str): adoptable, adopted, found
            page (int): Page number of results
        """
        params = {k: v for k, v in locals().items() if v is not None and k != "self"}
        params["type"] = "Dog"

        return self._make_request("animals", params=params)

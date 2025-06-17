from fastapi import FastAPI, APIRouter
import weaviate
from weaviate.auth import AuthApiKey
from weaviate.collections.classes.config import Property, DataType
from typing import List
import os



WEAVIATE_URL = os.getenv("WEAVIATE_URL")
WEAVIATE_API_KEY = os.getenv("WEAVIATE_API_KEY")
OPENAI_KEY = os.getenv("OPENAI_KEY")


router = APIRouter()

client = weaviate.connect_to_weaviate_cloud(
        cluster_url=WEAVIATE_URL,
        auth_credentials=AuthApiKey(WEAVIATE_API_KEY),
        headers={"X-OpenAI-Api-Key": OPENAI_KEY},
        skip_init_checks=True
    )

def fetch_unique_property_values(property_name: str) -> List[str]:
    
    seen = set()
    offset_size=0
    batch_size=500
    all_articles_collection = client.collections.get("Total_Articles")

    while True:
        all_objects = all_articles_collection.query.fetch_objects(
            return_properties=[property_name],
            limit=batch_size,
            offset=offset_size
        )
        if not all_objects.objects:
            break

        for obj in all_objects.objects:
            value = obj.properties.get(property_name)
            if value:
                seen.add(value.strip())
        offset_size += batch_size

    return sorted(seen)

@router.get("/locations", response_model=List[str])
def get_all_locations():
    return fetch_unique_property_values("location")

@router.get("/types", response_model=List[str])
def get_all_types():
    return fetch_unique_property_values("type")

@router.get("/sources", response_model=List[str])
def get_all_sources():
    return fetch_unique_property_values("source")

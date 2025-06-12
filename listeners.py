from fastapi import HTTPException, APIRouter
from pydantic import BaseModel, EmailStr
from datetime import datetime, timezone
from uuid import UUID
import weaviate
from weaviate.auth import AuthApiKey
from weaviate.collections.classes.config import Property, DataType
import os

WEAVIATE_URL = os.getenv("WEAVIATE_URL")
WEAVIATE_API_KEY = os.getenv("WEAVIATE_API_KEY")
OPENAI_KEY = os.getenv("OPENAI_KEY")

router = APIRouter()

class ListenerIn(BaseModel):
    query: str
    email: EmailStr

@router.post("/create_listener")
def create_listener(listener: ListenerIn):
    client = weaviate.connect_to_weaviate_cloud(
        cluster_url=WEAVIATE_URL,
        auth_credentials=AuthApiKey(WEAVIATE_API_KEY),
        headers={"X-OpenAI-Api-Key": OPENAI_KEY},
        skip_init_checks=True
    )
    if not client.collections.exists("Listeners"):
        client.collections.create(
            name="Listeners",
            properties=[
                Property(name="query", data_type=DataType.TEXT),
                Property(name="email", data_type=DataType.TEXT),
                Property(name="created_at", data_type=DataType.DATE),
            ]
        )
    listeners = client.collections.get("Listeners")
    listeners.data.insert(
        properties={
            "query": listener.query,
            "email": listener.email,
            "created_at": datetime.now().replace(tzinfo=timezone.utc).isoformat()
        }
    )
    print("✅ Listener Added.")
    client.close()
    return {"✅ status": "Added"}

@router.get("/get_all_listeners")
def get_all_listeners():
    client = weaviate.connect_to_weaviate_cloud(
        cluster_url=WEAVIATE_URL,
        auth_credentials=AuthApiKey(WEAVIATE_API_KEY),
        headers={"X-OpenAI-Api-Key": OPENAI_KEY},
        skip_init_checks=True
    )
    listeners = client.collections.get("Listeners")
    listeners_results = listeners.query.fetch_objects(
        return_properties=["query", "email", "created_at"],
        limit=10000
    )
    results = []
    for obj in listeners_results.objects:
        results.append({
            "id": obj.uuid,
            "query": obj.properties["query"],
            "email": obj.properties["email"],
            "created_at": obj.properties["created_at"]
        })
    print("✅ All Listeners.")
    client.close()
    return results

@router.delete("/delete_listener")
def delete_listener(listener_id: UUID):
    client = weaviate.connect_to_weaviate_cloud(
        cluster_url=WEAVIATE_URL,
        auth_credentials=AuthApiKey(WEAVIATE_API_KEY),
        headers={"X-OpenAI-Api-Key": OPENAI_KEY},
        skip_init_checks=True
    )
    listeners = client.collections.get("Listeners")
    try:
        listeners.data.delete_by_id(listener_id)
        print("✅ Listener Deleted.")
        client.close()
        return {"✅ status": "deleted"}
    except Exception:
        raise HTTPException(status_code=404, detail="Listener not found")

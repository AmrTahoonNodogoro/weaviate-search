from fastapi import FastAPI, Query
import weaviate
from weaviate.auth import AuthApiKey
from typing import List
from dotenv import load_dotenv
from datetime import datetime, timezone
from weaviate.collections.classes.filters import Filter
from uuid import UUID
from fastapi.exceptions import HTTPException

import os

# ENV variables
load_dotenv()

# ENV variables
WEAVIATE_URL = os.getenv("WEAVIATE_URL")
WEAVIATE_API_KEY = os.getenv("WEAVIATE_API_KEY")
OPENAI_KEY = os.getenv("OPENAI_KEY")
# WEAVIATE_URL = "https://1hgym4ylspqqei1dpfbe9a.c0.us-west3.gcp.weaviate.cloud"
# WEAVIATE_API_KEY = "FNTe5rJSAOmqg9oOD0WD4bNUbcLw8KbV9clr"
# OPENAI_KEY = "skprojPtSeGeprP_nueXHRuN6d4iCK7jqqkpkDVJ9qa46IywMKadVK7zXTf2Wz1MMz8aXgx6hnaQ2B4nT3BlbkFJxq0VuStSZ2sZS_6gJfIRIKmXJzfSXVFvPv4-o2RhaZDWoB41Iuhe0izPHUNWmThk_McYlTwykA"

client = weaviate.connect_to_weaviate_cloud(
    cluster_url=WEAVIATE_URL,
    auth_credentials=AuthApiKey(WEAVIATE_API_KEY),
    headers={"X-OpenAI-Api-Key": OPENAI_KEY},
    skip_init_checks=True)

app = FastAPI()


@app.get("/")
def read_root():
    return {"message": "Welcome to the Search API!"}


@app.get("/search_articles", response_model=List[dict])
def search_articles(q: str = Query(..., description="Search query string"),
                    date_from: str = Query(None, description="Start date in YYYY-MM-DD format"),
                    date_to: str = Query(None, description="End date in YYYY-MM-DD format")):

    try:
        all_articles_collection = client.collections.get("Total_Articles")

        filter_data = None
        date_filter = None
        if date_from or date_to:
            if date_from:
                from_dt = datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc)
                date_filter=Filter.by_property("date").greater_or_equal(from_dt)
                date_filter = date_filter

            if date_to:
                to_dt = datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc)
                to_filter=Filter.by_property("date").less_or_equal(to_dt)
                if date_filter:
                    date_filter = date_filter & to_filter 
                else:
                    date_filter = to_filter 
        text_filter=Filter.by_property("content").contains_all([q])
        filter_data=text_filter & date_filter  

        all_articles_results = all_articles_collection.query.bm25(
            query=q,
            query_properties=["content"],
            return_properties=["source","title", "url", "content","location","date","type"],
            limit=10000,
            filters=filter_data
        )

        results = []
        seen_urls = set()
        for obj in all_articles_results.objects:
            props = obj.properties
            content = props.get("content", "")
            url = props.get("url")
            normalized_content = content.lower().replace("-", " ")
            normalized_content = normalized_content.replace("\n", " ")
            normalized_query = q.lower().replace("-", " ")
            match_index = normalized_content.find(normalized_query)
            if url in seen_urls:
                continue
            # Get a snippet of 40 characters before and after the match
            start = max(match_index - 100, 0)
            end = min(match_index + len(q) + 100, len(content))
            match_context = content[start:end]

            results.append({
                "uuid":obj.uuid,
                "source":props.get("source"),
                "title": props.get("title"),
                "url": url,
                "match_context": match_context,
                "location":props.get("location"),
                "date":props.get("date").date(),
                "type":props.get("type")
            })
            seen_urls.add(url)

        return results

    except Exception as e:
        return [{"error": str(e)}]

@app.get("/get_article")
def get_article_by_uuid(uuid: UUID):

    try:

        all_articles_collection = client.collections.get("Total_Articles")


        try:
            obj = all_articles_collection.query.fetch_object_by_id(uuid)
            if obj:
                return {
                    "uuid": str(uuid),
                    "properties": obj.properties
                }
            else:
                raise HTTPException(status_code=404, detail="Article not found.")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
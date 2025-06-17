from fastapi import FastAPI, Query, Response
from listeners import router as listeners_router
from fetch_data import router as fetch_router
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

@app.head("/")
def head_root():
    return Response(status_code=200)

@app.get("/")
def read_root():
    return {"message": "Welcome to the Search API!"}


@app.get("/search_articles", response_model=List[dict])
def search_articles(q: str = Query(..., description="Search query string"),
                    date_from: str = Query(None, description="Start date in YYYY-MM-DD format"),
                    date_to: str = Query(None, description="End date in YYYY-MM-DD format"),
                    source: str = Query(None, description="Filter by source: CAPublicNotice or CEQAnet"),
                    type: str = Query(None, description="Filter by type of article"),
                    location: str = Query(None, description="Filter by location")):

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
            
        filter_data = text_filter & date_filter if date_filter else text_filter  
        
        if source:
            source_filter = Filter.by_property("source").equal(source)
            filter_data = filter_data & source_filter

        if type:
            type_filter = Filter.by_property("type").equal(type)
            filter_data = filter_data & type_filter

        if location:
            location_filter = Filter.by_property("location").equal(location)
            filter_data = filter_data & location_filter

        all_articles_results = all_articles_collection.query.bm25(
            query=q,
            query_properties=["content"],
            return_properties=["source","title", "url", "content","location","date","type"],
            limit=100000,
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


@app.get("/RAG_search_articles", response_model=List[dict])
def RAG_search_articles(q: str = Query(..., description="Search query string")):

    try:
        all_articles_collection = client.collections.get("Articles_OpenAI")

       
        all_articles_results = all_articles_collection.generate.near_text(
            query=q,
            return_properties=["title", "url"],
            limit=5,
            # distance=0.9,
            single_prompt="Answer the user's question as accurately as possible using ONLY the information in the {title} and {content} in only one sentance max 200 character",
            return_metadata=MetadataQuery(distance=True)
          
        )
       
        results = []
        seen_urls = set()
        for obj in all_articles_results.objects:
            props = obj.properties
            # content = props.get("content", "")
            url = props.get("url")

            if url in seen_urls:
                continue

            results.append({

                "title": props.get("title"),
                "url": url,
                "Single prompt result": {obj.generative.text},

            })
            seen_urls.add(url)
        print(len(results))
        return results

    except Exception as e:
        return [{"error": str(e)}]
 

app.include_router(listeners_router)
app.include_router(fetch_router)

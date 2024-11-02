from fastapi import FastAPI, HTTPException
import requests

app = FastAPI()

# Define the endpoint for fetching URL metadata
@app.get("/fetch_metadata/{url:path}")
async def fetch_metadata(url: str):
    # URL encode to match the Common Crawl format
    formatted_url = f"http://index.commoncrawl.org/CC-MAIN-2024-42-index?url={url}&output=json"
    
    try:
        # Fetch data from Common Crawl API
        response = requests.get(formatted_url)  
        response.raise_for_status()

        # Extract metadata from the response JSON
        metadata = response.json()

        # Return only the first few items as a sample
        sample_metadata = metadata[:5]  # Get first 5 entries as a preview

        if sample_metadata:
            return {"metadata": sample_metadata}
        else:
            raise HTTPException(status_code=404, detail="Metadata not found")
    
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Error fetching metadata: {str(e)}")

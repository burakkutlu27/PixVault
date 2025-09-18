import httpx
from typing import List, Dict


def search_unsplash(query: str, per_page: int, api_key: str) -> List[Dict[str, str]]:
    """
    Search for images on Unsplash using the API.
    
    Args:
        query: Search query string
        per_page: Number of results per page
        api_key: Unsplash API key
        
    Returns:
        List of dictionaries containing image data with keys: url, id, source
    """
    url = f"https://api.unsplash.com/search/photos?query={query}&per_page={per_page}"
    headers = {
        "Authorization": f"Client-ID {api_key}"
    }
    
    try:
        with httpx.Client() as client:
            response = client.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            results = []
            
            for photo in data.get("results", []):
                result = {
                    "url": photo.get("urls", {}).get("regular", ""),
                    "id": photo.get("id", ""),
                    "source": "unsplash"
                }
                results.append(result)
            
            return results
            
    except httpx.HTTPError as e:
        print(f"HTTP error occurred: {e}")
        return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []
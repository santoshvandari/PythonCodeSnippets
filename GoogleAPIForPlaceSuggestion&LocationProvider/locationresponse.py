import os,requests,asyncio

GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")

async def get_location_by_name(place_name:str):
    try:
        google_places_url="https://places.googleapis.com/v1/places:searchText"
        headers={
            'Content-Type': 'application/json',
            'x-Goog-Api-Key': GOOGLE_API_KEY,
            'x-Goog-FieldMask': 'places.location'
        }
        params = {
            "textQuery":place_name,
        }
        response = requests.post(google_places_url,params=params,headers=headers)
        response.raise_for_status()
        data = response.json()
        if "places" not in data or not data["places"]:
            return {"error": "No location found for the specified place"}
        return {
            "places": data["places"]
        }

    except Exception as err:
        return {"error": str(err)}
    
async def main():
    query = input("Enter your query for location search: ")
    result = await get_location_by_name(query)
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
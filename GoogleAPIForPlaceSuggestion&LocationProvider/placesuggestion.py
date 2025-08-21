
import os,requests,asyncio

GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
async def get_autocomplete_suggestions(query: str):
    try:
        google_places_url = "https://places.googleapis.com/v1/places:autocomplete"
        headers = {
            'Content-Type': 'application/json',
            'x-Goog-Api-Key': GOOGLE_API_KEY,
            'X-Goog-FieldMask': 'suggestions.placePrediction.text.text'
        }
        params = {
            "input": query
        }
        response = requests.post(google_places_url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        suggested_places = data.get("suggestions", [])
        if not suggested_places:
            return {"error": "No autocomplete suggestions found"}

        suggestions = list(set(prediction.get("placePrediction").get("text").get("text") for prediction in suggested_places if prediction.get("placePrediction")))
        return {"suggestions": suggestions}

    except Exception as err:
        return {"error": str(err)}
    
async def main():
    query = input("Enter your query for autocomplete suggestions: ")
    result = await get_autocomplete_suggestions(query)
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
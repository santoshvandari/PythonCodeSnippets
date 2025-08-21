import re,ast, json, uuid, time, datetime, asyncio
from pymongo import MongoClient

RUNNING_QUERIES = {}  # Store running queries with their run_id

ALLOWED_FIRST_METHODS = {"find", "aggregate"}
ALLOWED_CHAIN_METHODS = {
    "sort", "skip", "limit", "hint", "maxTimeMS", "collation", "batchSize", "comment", "allowDiskUse"
}
DISALLOWED_METHODS = {"insert", "update", "remove", "delete", "replace", "drop", "insertOne", "insertMany","updateOne", "updateMany", "deleteOne", "deleteMany", "bulkWrite"}

def get_mongo_database():
    client = MongoClient("mongodb://admin:admin@localhost:27017")
    database = client["mydb"]
    return database

def convert_mongo_syntax_to_python(s: str) -> str:
    """
    Converts MongoDB shell syntax to Python-evaluable syntax for ast.literal_eval
    - Quotes unquoted keys like $unwind, $match, etc.
    - Converts true/false/null
    - Handles new Date(...) by converting to ISO strings first
    """
    # Replace JavaScript booleans/null
    s = s.replace("true", "True").replace("false", "False").replace("null", "None")
    
    # Convert new Date(...) to ISO string placeholders that ast.literal_eval can handle
    def date_replacer(match):
        date_str = match.group(1)
        # Just return the date string - we'll process it later
        return f'"{date_str}"'
    
    s = re.sub(
        r'new\s+Date\s*\(\s*"(.*?)"\s*\)',
        date_replacer,
        s
    )

    # Quote Mongo keys (unquoted words followed by :)
    s = re.sub(r'([{,]\s*)(\$?[a-zA-Z_][\w$]*)(\s*:)', r'\1"\2"\3', s)

    return s

def process_parsed_args(obj):
    """
    Post-process parsed arguments to convert date strings to datetime objects
    This handles the conversion of ISO date strings to proper datetime objects
    """
    if isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            # Check if this looks like a date field with date operators
            if key in ['$gte', '$lt', '$lte', '$gt', '$ne', '$eq'] and isinstance(value, str):
                if is_iso_date_string(value):
                    result[key] = parse_iso_date_string(value)
                else:
                    result[key] = value
            else:
                result[key] = process_parsed_args(value)
        return result
    elif isinstance(obj, list):
        return [process_parsed_args(item) for item in obj]
    else:
        return obj

def is_iso_date_string(s):
    """Check if string looks like an ISO date"""
    if not isinstance(s, str):
        return False
    # Check for ISO date format
    return re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?$', s) is not None

def parse_iso_date_string(date_str):
    """Parse ISO date string to datetime object"""
    try:
        if date_str.endswith('Z'):
            date_str = date_str[:-1] + '+00:00'
        return datetime.datetime.fromisoformat(date_str)
    except ValueError:
        return date_str  # Return original if parsing fails

def split_args(args_str: str):
    """Split multiple arguments like: {a}, {b} or [{...}, {...}]"""
    depth = 0
    current = ''
    result = []
    in_string = False
    escape = False
    for char in args_str:
        if char == '"' and not escape:
            in_string = not in_string
        if not in_string and char == ',' and depth == 0:
            result.append(current)
            current = ''
        else:
            current += char
            if not in_string:
                if char in ['{', '[']:
                    depth += 1
                elif char in ['}', ']']:
                    depth -= 1
        escape = (char == '\\') and not escape
    if current:
        result.append(current)
    return result

def parse_arguments_safely(arg_str):
    """Parse arguments with proper date handling"""
    try:
        args = split_args(arg_str)
        parsed_args = []
        for arg in args:
            if arg.strip():
                # First convert mongo syntax to python
                converted = convert_mongo_syntax_to_python(arg.strip())
                # Parse with ast.literal_eval
                parsed = ast.literal_eval(converted)
                # Post-process to handle dates
                processed = process_parsed_args(parsed)
                parsed_args.append(processed)
        return parsed_args
    except Exception as ex:
        raise ValueError(f"Failed to parse arguments: {ex}")

# Extract balanced brackets from a string
def extract_balanced_brackets(text: str):
    """Extract string up to the matching closing parenthesis, return (inside, rest)."""
    depth = 0
    in_string = False
    escape = False
    result = ''
    for i, char in enumerate(text):
        result += char
        if char == '"' and not escape:
            in_string = not in_string
        if not in_string:
            if char in '([{':
                depth += 1
            elif char in ')]}':
                depth -= 1
            if depth == 0:
                return result.strip()[1:-1], text[i+1:].strip()
        escape = (char == '\\') and not escape
    raise ValueError("Unbalanced brackets in query")

# Mongo DB AI Assistant Export Query Result 
def parse_query_string(query: str):
    """
    Parse Mongo shell-style query like:
    db.collection.find({...}, {...})
    db.collection.aggregate([...])
    """
    query = query.strip().rstrip(";").replace("true", "True").replace("false", "False")

    # Reject any disallowed keywords early
    for bad in DISALLOWED_METHODS:
        if f".{bad}" in query:
            raise Exception(
                f"Only read operations are allowed. '{bad}' is not permitted."
            )
    
    # Regex to match: db.collection.method(...)
    top_match = re.match(r"^db\.(\w+)\.([a-zA-Z]+)\s*\((.*)", query, re.DOTALL)
    if not top_match:
        raise Exception(
            "Invalid query syntax. Start with db.collection.find(...) or aggregate(...)"
        )

    collection = top_match.group(1)
    method = top_match.group(2)

    if method not in ALLOWED_FIRST_METHODS:
        raise Exception(
            f"Only 'find' and 'aggregate' read operations are allowed. Found: '{method}'"
        )
    
    # Extract method calls including chains (balance brackets)
    calls = []
    remainder = query[query.find(f".{method}(") + len(f".{method}"):]
    while remainder:
        method_match = re.match(r"\s*\((.*)", remainder, re.DOTALL)
        if not method_match:
            break

        arg_str, remainder = extract_balanced_brackets(remainder)
        calls.append((method, arg_str.strip()))
        if '.' in remainder:
            next_method_match = re.match(r"\.(\w+)\s*\((.*)", remainder, re.DOTALL)
            if not next_method_match:
                break
            method = next_method_match.group(1)
            if method not in ALLOWED_CHAIN_METHODS:
                raise Exception(
                    f"Chained method '{method}' is not allowed. Only safe read methods can be used."
                )
            remainder = remainder[remainder.find(f"{method}(") + len(method):]
        else:
            break

    # Parse arguments safely with date handling
    parsed_calls = []
    for method, arg_str in calls:
        try:
            parsed_args = parse_arguments_safely(arg_str)
        except Exception as ex:
            raise Exception(
                f"Failed to parse arguments for {method}(): {str(ex)}"
            )
        parsed_calls.append((method, parsed_args))

    return collection, parsed_calls




def export_query_result(data: str):
    """Initiates a query, returns a run_id immediately, and starts the stream in background."""
    run_id = str(uuid.uuid4())
    try:
        if not data:
            raise Exception("Query cannot be empty")
        collection_name, operations = parse_query_string(data)
        db = get_mongo_database()
        collection = db[collection_name]

        async def get_cursor():
            method_name, args = operations[0]
            if method_name == "find":
                filter_doc = args[0] if len(args) > 0 else {}
                projection = args[1] if len(args) > 1 else {"_id": 0}
                if "_id" not in projection:
                    projection["_id"] = 0
                cursor = collection.find(filter_doc, projection)
            elif method_name == "aggregate":
                pipeline = args[0] if len(args) > 0 else []
                if isinstance(pipeline, list) and all(isinstance(stage, dict) for stage in pipeline):
                    has_project = any('$project' in stage for stage in pipeline)
                    if not has_project:
                        pipeline.append({"$project": {"_id": 0}})
                cursor = collection.aggregate(pipeline)
            else:
                raise Exception("Only 'find' and 'aggregate' operations are allowed")

            # Apply chained methods
            for method_name, method_args in operations[1:]:
                if method_name == "sort":
                    cursor = cursor.sort(list(method_args[0].items()))
                elif method_name == "limit":
                    cursor = cursor.limit(method_args[0])
                elif method_name == "skip":
                    cursor = cursor.skip(method_args[0])
                elif method_name == "hint":
                    cursor = cursor.hint(method_args[0])
                elif method_name == "maxTimeMS":
                    cursor = cursor.max_time_ms(method_args[0])
                elif method_name == "collation":
                    cursor = cursor.collation(method_args[0])
                elif method_name == "batchSize":
                    cursor = cursor.batch_size(method_args[0])
                elif method_name == "comment":
                    cursor = cursor.comment(method_args[0])
                else:
                    raise Exception(f"Unsupported chained method: {method_name}")
            return cursor

        cancel_event = asyncio.Event()
        RUNNING_QUERIES[run_id] = {
            "cursor": get_cursor,  # defer until stream call
            "cancel_event": cancel_event,
            "timestamp": time.time()
        }

        
        return run_id

    except Exception as e:
        print(f"Error preparing query: {e}")
        return None
    
# Streaming response
async def safe_stream_response(run_id, future_cursor, cancel_event):
    """ Stream MongoDB Cursor in Json Array"""
    yield '{"run_id": "' + run_id + '","data": [' 
    first = True
    try:
        cursor = await future_cursor() # wait for mongodb to prepare cursor

        if run_id in RUNNING_QUERIES:
            RUNNING_QUERIES[run_id]["active_cursor"] = cursor
        async for document in cursor:
            if cancel_event.is_set():
                break
            document.pop('_id', None)  # Remove _id field from response
            if not first:
                yield ","
            yield json.dumps(document, default=str)  # Convert document to JSON string
            first = False
    except Exception as e:
        print(f"Error streaming results for {run_id}: {e}")
    finally:
        RUNNING_QUERIES.pop(run_id, None)
        try:
            cursor.close()
        except:
            print(f"Error closing cursor for run_id={run_id}")
        yield "]}"  # Close JSON
        print(f"Run ID: {run_id} finished")



def stream_query_result(run_id: str):
    entry = RUNNING_QUERIES.get(run_id)
    if not entry:
        return "Invalid or expired run_id"

    try:
        get_cursor = entry["cursor"]
        cancel_event = entry["cancel_event"]
        stream = safe_stream_response(run_id, get_cursor, cancel_event)
        return stream
    except Exception as e:
        print(f"Error streaming results for {run_id}: {e}")
        return None




def terminate_current_run(run_id: str):
    """
    Terminate the current MongoDB run.
    """
    entry = RUNNING_QUERIES.pop(run_id, None)
    if not entry:
        return {"message":"Process Already Completed", "run_id": run_id}
    entry["cancel_event"].set()
    if "active_cursor" in entry:
        try:
            entry["active_cursor"].close()
            print(f"Cursor closed for run_id={run_id}")
        except Exception as e:
            print(f"Error closing cursor for run_id={run_id}: {e}")
    else:
        print(f"No active cursor found for run_id={run_id}")

    return {"message":"Cancellation Requested", "run_id": run_id}

async def main():
    query = input("Enter your MongoDB query: ")
    print(f"Processing query: {query}")
    run_id=export_query_result(query)
    if not run_id:
        print("Failed to start query.")
        return
    print(f"Query started successfully with run_id: {run_id}")
    stream = stream_query_result(run_id)
    if not stream:
        print("Failed to stream query results.")
        return
    async for document in stream:
        print(f"Streaming document for run_id={run_id}: {document}")
    print(f"Finished streaming documents for run_id={run_id}")

    # if like to terminate in middle if need
    # terminate_current_run(run_id)


if __name__ == "__main__":
    asyncio.run(main())
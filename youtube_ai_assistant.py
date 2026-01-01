from youtube_transcript_api import YouTubeTranscriptApi
import http.client
import re
import boto3
import time
import json
import datetime
import pytz
import random
import logging
import requests

# Set up logging configuration
logging.basicConfig(
    filename='my_log_file.log',  # Log file name
    level=logging.DEBUG,  # Log level (can be DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
    filemode='w'  # 'w' means write (overwrites the log file), 'a' would append
)

#BEDROCK_MODEL_ID_SONNET = "anthropic.claude-3-5-sonnet-20240620-v1:0"
#BEDROCK_MODEL_ID = "anthropic.claude-3-5-sonnet-20240620-v1:0"
#BEDROCK_MODEL_ID = "anthropic.claude-3-5-sonnet-20241022-v2:0"
BEDROCK_MODEL_ID = "us.anthropic.claude-3-haiku-20240307-v1:0"
BEDROCK_MAX_TOKENS = 4096
BEDROCK_TEMPERATURE = 0.0
BEDROCK_TOP_P = 0.9

bedrock_client = boto3.client('bedrock-runtime', region_name='us-west-2')
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

table = dynamodb.Table('youtube_video_data')
bucket_name = 'your_bucket_name' #replace with your bucket name

CHANNEL_HOST = "www.youtube.com"
CHANNEL_PATH = "/@AWSEventsChannel/videos"

def extract_view_count(video_url):
    """Scrape the view count of a YouTube video from its page source."""
    response = requests.get(video_url)
    html_content = response.text
    
    # Regular expression to extract the view count from the page's JavaScript
    match = re.search(r'"viewCount":"(\d+)"', html_content)
    if match:
        return int(match.group(1))  # Return the view count as an integer
    return 0  # Default to 0 if not found

def update_dynamodb(video_url, view_count):
    """Update the DynamoDB table with the view count for a video."""

    # Update the item based on the video_url as the key
    try:
        table.update_item(
            Key={
                'video_url': video_url
            },
            UpdateExpression="SET view_count = :view_count",
            ExpressionAttributeValues={":view_count": view_count},
            ReturnValues="UPDATED_NEW"
        )
        print(f"Updated {video_url} with {view_count} views.")
    except Exception as e:
        print(f"Error updating DynamoDB for {video_url}: {str(e)}")

def get_video_urls():
    last_evaluated_key = None

    while True:
        if last_evaluated_key:
            response = table.scan(
                    ExclusiveStartKey=last_evaluated_key,
                    FilterExpression="event_year = :event_year",
                    ExpressionAttributeValues={
                        ":event_year": "2024"
                    }
                )
        else:
            response = table.scan(
                    FilterExpression="event_year = :event_year",
                    ExpressionAttributeValues={
                        ":event_year": "2024"
                    }
                )

        # Loop through each item in the response
        for item in response.get('Items', []):
            video_url = item.get('video_url')
            if video_url:
                print(f"Processing {video_url}...")
                # Extract the view count for the video
                view_count = extract_view_count(video_url)
                # Update the DynamoDB table with the view count
                update_dynamodb(video_url, view_count)

        # Check if there's another page of results
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break  # Exit loop if there are no more pages

        # Sleep for a short time to avoid overloading the API and DynamoDB
        time.sleep(1)

def extract_json_from_html(html_content):
    match = re.search(r'var ytInitialData = ({.*?});', html_content)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            return None
    return None

def get_reinvent_2024_videos():
    video_info = []
    continuation_token = None
    
    # Initial request
    conn = http.client.HTTPSConnection(CHANNEL_HOST)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    
    try:
        # Get initial page
        conn.request("GET", CHANNEL_PATH, headers=headers)
        response = conn.getresponse()
        initial_data = response.read().decode('utf-8')
        
        # Extract initial videos and continuation token
        json_data = extract_json_from_html(initial_data)
        if json_data:
            tabs = json_data.get('contents', {}).get('twoColumnBrowseResultsRenderer', {}).get('tabs', [])
            for tab in tabs:
                if 'tabRenderer' in tab and tab['tabRenderer'].get('selected', False):
                    contents = tab['tabRenderer'].get('content', {}).get('richGridRenderer', {}).get('contents', [])
                    
                    for content in contents:
                        if 'richItemRenderer' in content:
                            video_renderer = content['richItemRenderer']['content'].get('videoRenderer', {})
                            if video_renderer:
                                title = video_renderer.get('title', {}).get('runs', [{}])[0].get('text', '')
                                if "AWS re:Invent 2024" in title:
                                    video_id = video_renderer.get('videoId', '')
                                    video_info.append({
                                        'url': f"https://www.youtube.com/watch?v={video_id}",
                                        'title': title
                                    })
                        elif 'continuationItemRenderer' in content:
                            continuation_token = content['continuationItemRenderer']['continuationEndpoint']['continuationCommand']['token']
        
        # Handle pagination with limit of 35 iterations
        page_count = 0
        while continuation_token and page_count < 35:
            print(f"Fetching page {page_count + 1}... (Found {len(video_info)} videos so far)")
            
            post_data = {
                "context": {
                    "client": {
                        "clientName": "WEB",
                        "clientVersion": "2.20240101.01.00",
                    }
                },
                "continuation": continuation_token
            }
            
            headers['Content-Type'] = 'application/json'
            conn = http.client.HTTPSConnection(CHANNEL_HOST)
            conn.request(
                "POST",
                "/youtubei/v1/browse?key=AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8",
                json.dumps(post_data),
                headers
            )
            
            response = conn.getresponse()
            data = json.loads(response.read().decode('utf-8'))
            
            continuation_token = None
            items = data.get('onResponseReceivedActions', [{}])[0].get('appendContinuationItemsAction', {}).get('continuationItems', [])
            
            for item in items:
                if 'richItemRenderer' in item:
                    video_renderer = item['richItemRenderer']['content'].get('videoRenderer', {})
                    if video_renderer:
                        title = video_renderer.get('title', {}).get('runs', [{}])[0].get('text', '')
                        if "AWS re:Invent 2024" in title:
                            video_id = video_renderer.get('videoId', '')
                            video_info.append({
                                'url': f"https://www.youtube.com/watch?v={video_id}",
                                'title': title
                            })
                elif 'continuationItemRenderer' in item:
                    continuation_token = item['continuationItemRenderer']['continuationEndpoint']['continuationCommand']['token']
            
            page_count += 1
            time.sleep(1)  # Be nice to YouTube's servers
            
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        conn.close()
    
    return video_info

# Function to fetch the YouTube transcript using youtube-transcript-api
def fetch_transcript(youtube_video_url):
    # Extract the video ID from the YouTube URL
    video_id = youtube_video_url.split("v=")[1]
    
    # Fetch the transcript using the YouTubeTranscriptApi
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id)
    except Exception as e:
        print(f"Error fetching transcript: {e}")
        return []
    
    return transcript

# Generator function to extract sentences from the YouTube transcript
def get_sentences_from_youtube_transcript(transcript):
    current_sentence = []
    for entry in transcript:
        text = entry["text"]
        current_sentence.append(text)

        # Yield the sentence when it ends with a period
        if text.endswith("."):
            yield " ".join(current_sentence).replace("\n", " ")
            current_sentence = []
    
    # Yield any remaining sentence if there's no period at the end
    if current_sentence:
        yield " ".join(current_sentence).replace("\n", " ")

# Process the YouTube video URL
def process_youtube_url(youtube_video_url):
    try:
        # Fetch transcript for the provided YouTube video URL
        transcript = fetch_transcript(youtube_video_url)

        if not transcript:
            print(f"No transcript available for video: {youtube_video_url}")
            return

        # Create the header row
        transcript_data = "start - text\n"

        # Loop through the data and append each row with converted start time (minutes:seconds) and duration
        for entry in transcript:
            start_seconds = entry['start']
            minutes = int(start_seconds // 60)  # Calculate full minutes
            seconds = round(start_seconds % 60, 2)  # Calculate remaining seconds, rounded to 2 decimal places
            start_time = f"{minutes:02}:{int(seconds):02}"  # Convert to 'minutes:seconds' format (integer seconds)

            # Append the converted start time and duration to the CSV data
            transcript_data += f"{start_time} - \"{entry['text']}\"\n"
        
        # Get sentences from the transcript using the generator
        sentences = list(get_sentences_from_youtube_transcript(transcript))

        transcript_sentences = "\n".join(sentences)
        return transcript_data, transcript_sentences
    
    except Exception as e:
        print(f"Error processing video {youtube_video_url}: {e}")
        transcript_data = None
        transcript_sentences = None
        return transcript_data, transcript_sentences

# Function to get the YouTube video URLs from a playlist without API key
def get_youtube_playlist_urls(playlist_id, max_retries=3):
    video_urls = []
    continuation_token = None
    retries = 0

    while True:
        try:
            conn = http.client.HTTPSConnection("www.youtube.com")
            
            if continuation_token:
                post_data = {
                    "continuation": continuation_token,
                    "context": {
                        "client": {
                            "clientName": "WEB",
                            "clientVersion": "2.20230531.01.00"
                        }
                    }
                }
                headers = {
                    "Content-Type": "application/json",
                    "X-YouTube-Client-Name": "1",
                    "X-YouTube-Client-Version": "2.20230531.01.00"
                }
                conn.request("POST", "/youtubei/v1/browse?key=AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8", json.dumps(post_data), headers)
            else:
                conn.request("GET", f"/playlist?list={playlist_id}")

            response = conn.getresponse()
            data = response.read().decode('utf-8')

            if continuation_token:
                json_data = json.loads(data)
                items = json_data.get('onResponseReceivedActions', [{}])[0].get('appendContinuationItemsAction', {}).get('continuationItems', [])
            else:
                initial_data_match = re.search(r"var ytInitialData = ({.*?});", data)
                if initial_data_match:
                    json_data = json.loads(initial_data_match.group(1))
                    items = json_data.get('contents', {}).get('twoColumnBrowseResultsRenderer', {}).get('tabs', [])[0].get('tabRenderer', {}).get('content', {}).get('sectionListRenderer', {}).get('contents', [])[0].get('itemSectionRenderer', {}).get('contents', [])[0].get('playlistVideoListRenderer', {}).get('contents', [])
                else:
                    items = []

            for item in items:
                if 'playlistVideoRenderer' in item:
                    video_id = item['playlistVideoRenderer']['videoId']
                    video_urls.append(f"https://www.youtube.com/watch?v={video_id}")

            if not items:
                print("No more videos to fetch.")
                break

            continuation_token = items[-1].get('continuationItemRenderer', {}).get('continuationEndpoint', {}).get('continuationCommand', {}).get('token')
            
            if not continuation_token:
                print("No continuation token found. Ending fetch.")
                break

            print(f"Found {len(video_urls)} videos so far. Continuing to next page...")
            conn.close()
            retries = 0
            time.sleep(1)

        except Exception as e:
            print(f"Error occurred: {e}. Retrying...")
            retries += 1
            if retries >= max_retries:
                print(f"Max retries reached. Stopping.")
                break
            time.sleep(5)

    return video_urls

# Function to get video details: title, channel name, upload date, and duration
def get_video_details(video_url):
    # Set up the connection to YouTube for video page
    conn = http.client.HTTPSConnection("www.youtube.com")

    # Send the GET request to the video URL
    conn.request("GET", video_url)

    # Get the response from the server
    response = conn.getresponse()
    data = response.read()

    # Close the connection
    conn.close()

    # Decode the HTML content
    html_content = data.decode('utf-8')

    # Extract the title of the video using regex
    title_match = re.search(r'\"title\":\"([^\"]+)\"', html_content)
    title = title_match.group(1) if title_match else "N/A"

    # Extract the channel name using regex
    channel_name_match = re.search(r'\"author\":\"([^\"]+)\"', html_content)
    channel_name = channel_name_match.group(1) if channel_name_match else "N/A"

    # Extract the upload date using regex (in ISO 8601 format)
    upload_date_match = re.search(r'<meta itemprop="datePublished" content="([^\"]+)">', html_content)
    upload_date = upload_date_match.group(1) if upload_date_match else "N/A"

    # Extract the video duration using regex (in ISO 8601 format)
    duration_match = re.search(r'<meta itemprop="duration" content="([^\"]+)">', html_content)
    duration = duration_match.group(1) if duration_match else "N/A"

    # Format the duration into a human-readable format (e.g., PT1H2M30S -> 1 hour, 2 minutes, 30 seconds)
    if duration != "N/A":
        match = re.match(r"PT(\d+H)?(\d+M)?(\d+S)?", duration)
        hours = match.group(1) if match.group(1) else ''
        minutes = match.group(2) if match.group(2) else ''
        seconds = match.group(3) if match.group(3) else ''
        formatted_duration = f"{hours} {minutes} {seconds}".strip()
    else:
        formatted_duration = "N/A"

    return {
        'title': title,
        'channel_name': channel_name,
        'upload_date': upload_date,
        'duration': formatted_duration
    }

# Function to store data in DynamoDB
def store_video_data_in_dynamodb(playlist_url, video_url, title, event_name, event_year, channel_name, upload_date, duration, transcript, transcript_sentences):
    # Get the current time in UTC and convert it to EST (US Eastern time)
    utc_now = datetime.datetime.now(pytz.UTC)  # Current time in UTC
    est_timezone = pytz.timezone('US/Eastern')  # EST timezone
    est_now = utc_now.astimezone(est_timezone)  # Convert to EST
    
    try:
        # Use update_item to either update existing data or insert a new item if it doesn't exist
        response = table.update_item(
            Key={
                'video_url': video_url  # Assuming video_url is the primary key
            },
            UpdateExpression="""
                SET playlist_url = :playlist_url, 
                    title = :title,
                    event_name = :event_name,
                    event_year = :event_year,
                    channel_name = :channel_name,
                    upload_date = :upload_date,
                    #duration = :duration, 
                    transcript = :transcript,
                    transcript_sentences = :transcript_sentences,
                    updated_date = :updated_date
            """,  # Only update the fields provided
            ExpressionAttributeValues={
                ':playlist_url': playlist_url,
                ':title': title,
                ':event_name': event_name,
                ':event_year': event_year,
                ':channel_name': channel_name,
                ':upload_date': str(upload_date),
                ':duration': str(duration),
                ':transcript': str(transcript),
                ':transcript_sentences': str(transcript_sentences),
                ':updated_date': str(est_now.strftime('%Y-%m-%dT%H:%M:%S'))
            },
            ExpressionAttributeNames={
                '#duration': 'duration'  # Alias the reserved keyword `duration`
            }
        )

        print(f"Data for {video_url} successfully updated or inserted.")
    except Exception as e:
        print(f"Error updating or inserting data for {video_url}: {e}")

# Function to upload the title and transcript to S3
def upload_to_s3(video_url, title, transcript):
    # Extract video ID from the YouTube URL
    video_id = video_url.split("v=")[1]  # YouTube video ID
    
    # Combine title and transcript content into a single string
    combined_content = f"Title: {title}\n\nTranscript:\n{transcript}"
    
    # Generate a file name for the video
    file_name = f"video-{video_id}.txt"  # Using video ID in the file name
    
    # Upload the combined content (title + transcript) to S3
    s3_client.put_object(
        Bucket=bucket_name,
        Key=f"youtube_transcripts/{file_name}",  # Store inside 'youtube_transcripts/' folder
        Body=combined_content,
        ContentType='text/plain'
    )
    print(f"File uploaded to S3: youtube_transcripts/{file_name}")

def invoke_bedrock_model(prompt):
    logging.info(f"Inside invoke bedrock method")
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": BEDROCK_MAX_TOKENS,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": BEDROCK_TEMPERATURE,
        "top_p": BEDROCK_TOP_P
    }

    retries = 5
    current_model_id = BEDROCK_MODEL_ID  # Initially set to the default model

    for attempt in range(retries):
        try:
            response = bedrock_client.invoke_model(
                body=json.dumps(body),
                modelId=current_model_id  # Use the current model ID
            )

            response_body = json.loads(response.get("body").read())
            response_body1 = response_body.get("content")[0]['text']
            print(response_body1)

            # Step 1: Clean the text to remove problematic characters (like unescaped newlines or control characters)
            cleaned_text = re.sub(r'\\[ntrbf]', '', response_body1)  # Remove any escaped newline, tab, etc.
            cleaned_text = re.sub(r'[\x00-\x1f\x7f]', '', cleaned_text)  # Remove control characters like \n, \r, etc.


            # Define the regex pattern to extract JSON
            pattern = r'\{.*\}'

            # Step 3: Extract the JSON part using regex
            match = re.search(pattern, cleaned_text, re.DOTALL)  # re.DOTALL ensures multiline matching
            if match:
                json_string = match.group(0)  # Extract the matched JSON string
            else:
                raise ValueError("No valid JSON found in the input text")

            # Parse the JSON string into a Python dictionary
            json_data = json.loads(json_string)

            # Output the extracted JSON
            #print(json_data)

            return json_data

        except Exception as e:
            
            if "Input is too long for requested model" in str(e):
                # If the input is too long, switch to the backup model
                print("Input is too long for the current model, switching to BEDROCK_MODEL_ID_SONNET...")
                #current_model_id = BEDROCK_MODEL_ID_SONNET  # Switch model to BEDROCK_MODEL_ID_SONNET
                backoff_time = (2 ** attempt + random.uniform(0, 1)) * 10  # Increase the backoff multiplier
                time.sleep(backoff_time)
                
            elif "Too many tokens per min" in str(e):  # Throttling error
                # Handle throttling with exponential backoff
                #current_model_id = BEDROCK_MODEL_ID
                backoff_time = (2 ** attempt + random.uniform(0, 1)) * 10  # Increase the backoff multiplier
                print(f"Throttling detected. Retrying in {backoff_time:.2f} seconds...")
                time.sleep(backoff_time)

            elif "No valid JSON found in the input text" in str(e):
                print(f"No valid JSON found in the input text")

    print("Max retries reached, returning None.")
    return None

def check_video_exists_in_dynamodb(video_url):
    try:
        table = dynamodb.Table('youtube_video_data')
        
        # Query the DynamoDB table to check if the video_url exists
        response = table.get_item(
            Key={
                'video_url': video_url
            }
        )

        # Check if the item exists and if 'transcript_sentences' is present
        if 'Item' in response:
            # Check if the 'transcript_sentences' attribute is in the response item
            if 'transcript_sentences' in response['Item']:
                return True  # Video exists and transcript_sentences is available
            else:
                return False  # Video exists, but transcript_sentences is not available
        else:
            return False  # Video doesn't exist
    except Exception as e:
        print(f"Error checking video {video_url}: {e}")
        return False  # In case of error, assume the video doesn't exist

# Function to handle the 'generate_summary' process with pagination and checking for missing summary
def generate_summary():
    try:
        #prompt = "Please provide a comprehensive summary of the following content, highlighting the key points and important details."

        prompt = """
            Please analyze the following transcript and extract the details as outlined below. Provide the information in the requested JSON format.
            Ensure that **every** attribute in the output JSON is populated, even if some information is not explicitly mentioned. 
            If a particular detail cannot be determined, make sure to explicitly mention that in the corresponding field with a note indicating that 
            the information was not available.

            If multiple customers, presenters, industries, use cases, problem statements, or solutions are presented in the transcript, 
            capture **all** of them and structure them accordingly in the output JSON.

            - **Customer Name**: Identify the name of the customer or the company name. If multiple customers or companies are mentioned, 
                capture each one. If the company name is explicitly mentioned in the introduction or elsewhere in the transcript 
                (such as in the presenter's title or role), use that as the customer name.
                - If the company name or customer name is not explicitly stated, infer it from the context, such as:
                    - The presenter's title (e.g., "SVP of Products for [CompanyName]") or their reference to their company in the discussion.
                    - The mention of any **products** or **services** tied to a specific company.
                - If neither is mentioned or it cannot be inferred, mark it as **"Not Available"**.

            - Presenter Name & Title: Identify the name and title of each presenter. If multiple people are presenting at various stages of the 
              transcript, capture the name and title of each individual. If not explicitly stated, infer the presenter's name and title based on the 
              context or other parts of the transcript where this information may be mentioned.
              If the information cannot be determined, mark it as "Not Available".

            - Industry: Specify the industry the customer belongs to. If multiple industries are mentioned, capture each one.
              If the industry is not explicitly mentioned in the introduction,
              infer it from the context or details discussed throughout the transcript (e.g., company products, services, or sector-related keywords).
              If the industry is not identifiable, mark it as "Not Available".

            - Use Case: Describe the use case the customer presented. If multiple use cases are mentioned, capture each one.
              If the use case is unclear or not mentioned, note that in the output JSON as "Not Available".
              
            - Problem Statement: Highlight the key problem(s) the customer described. If multiple problem statements are mentioned, capture each one.
              If no problem is explicitly stated, provide an inference based on the conversation or mark it as "Not Available".

            - Solution: Detail the solution(s) the customer proposed, formatted in a paragraph style with headers.
              If multiple solutions are mentioned, capture each one.
              If no solution is proposed or discussed, state that in the output JSON as "Not Available".

            - AWS Services: Extract all AWS services explicitly mentioned and discussed in detail within the transcript. Only capture services 
              if the discussion about the service is in-depth or detailed. If a service is merely mentioned in passing without further explanation, do not capture it.
              For each service, include:
              - The name of the service
              - The timestamp (start time) when the service is first introduced
              - The duration of the discussion about the service (how long the discussion lasts in seconds)
              If the information is not available or no AWS services are mentioned, mark it as "Not Available".

            - Summary: Provide a comprehensive summary of the transcript, highlighting the key points and important details.
              Ensure that this summary covers the essence of the conversation, including problems, solutions, and relevant details. 
              If the summary is unclear, provide the best interpretation possible.
            
            - Key Points: List important points mentioned in the transcript. For each key point, include the corresponding time stamp and time duration.
              If key points cannot be captured, indicate that in the output with a "Not Available" entry.

            Ensure that the extracted information is structured as a JSON object, with **every** field populated according to the transcript 
            content. If any detail cannot be extracted or inferred, mark that field with "Not Available" and provide a brief explanation where necessary.

            Output JSON Format:
            ```json
            {
                "customer_names": [
                    "string",  // Capture all customer names if multiple customers are mentioned. If not available, mark as "Not Available"
                ],
                "presenter_details": [
                    {
                        "name": "string",  // If not available, mark as "Not Available"
                        "title": "string"  // If not available, mark as "Not Available"
                    }
                ],
                "industries": [
                    "string",  // Capture all industries if multiple industries are mentioned. If not available, mark as "Not Available"
                ],
                "use_cases": [
                    "string",  // Capture all use cases if multiple use cases are mentioned. If not available, mark as "Not Available"
                ],
                "problem_statements": [
                    "string",  // Capture all problem statements if multiple problem statements are mentioned. If not available, mark as "Not Available"
                ],
                "solutions": [
                    "string",  // Capture all solutions if multiple solutions are mentioned. If not available, mark as "Not Available"
                ],
                "aws_services": [
                    {
                        "time_stamp": "string",
                        "time_duration": "string",
                        "service_name": "string"
                    }
                ],
                "summary": "string",  // Provide a comprehensive summary of the content. If not available, mark as "Not Available"
                "key_points": [
                    {
                        "time_stamp": "string",
                        "time_duration": "string",
                        "point": "string"
                    }
                ]
            }
        """

        table = dynamodb.Table('youtube_video_data')

        # Initialize the scan with pagination
        last_evaluated_key = None
        
        while True:
            # Scan the DynamoDB table to retrieve all video data
            if last_evaluated_key:
                response = table.scan(
                    ExclusiveStartKey=last_evaluated_key,
                    FilterExpression="event_year = :event_year AND attribute_not_exists(customer_names)",
                    ExpressionAttributeValues={
                        ":event_year": "2024"
                    }
                )

            else:
                response = table.scan(
                    FilterExpression="event_year = :event_year AND attribute_not_exists(customer_names)",
                    ExpressionAttributeValues={
                        ":event_year": "2024"
                    }
                )


            print(f"Scanned {len(response['Items'])} items.")
            
            # Loop through each item in the DynamoDB table
            for item in response['Items']:
                # Check if the record has a 'summary' field or if it's empty
                #if not item.get('customer_names'):  # If no summary exists
                if not item.get('customer_names') and item.get('transcript'):  # If no summary exists
                #if item.get('summary'):  # If no summary exists

                    print(f"Processing video URL: {item['video_url']}")
                    # Get the transcript and concatenate it with "Prompt is ..."
                    transcript = item.get('transcript', '')

                    if transcript:
                        prompt = f"{prompt} - Transcript: {transcript}"

                        # Call the generate_summary function with the concatenated prompt
                        transcript_insights = invoke_bedrock_model(prompt)

                        if transcript_insights:
                            # Prepare the update expression and expression values
                            update_expression = "set "
                            expression_values = {}

                            # Iterate over all the fields in the item and create the update expression
                            for key, value in transcript_insights.items():
                                # Construct the update expression for the field
                                if isinstance(value, list):
                                    update_expression += f"{key} = :{key}, "
                                    expression_values[f":{key}"] = value
                                else:
                                    update_expression += f"{key} = :{key}, "
                                    expression_values[f":{key}"] = value

                            # Remove trailing comma and space from the UpdateExpression
                            update_expression = update_expression.rstrip(", ")

                            # Perform the update in DynamoDB
                            try:
                                response1 = table.update_item(
                                    Key={'video_url': item['video_url']},  # Partition key: video_url
                                    UpdateExpression=update_expression,
                                    ExpressionAttributeValues=expression_values
                                )
                                print(f"\n\n Generated and stored summary for video url: {item['video_url']}")
                            except Exception as e:
                                print("Error updating item:", e)
                    else:
                        print(f"No insights generated for video URL: {item['video_url']}")

            # If there's a LastEvaluatedKey, it means more records exist, so continue scanning
            last_evaluated_key = response.get('LastEvaluatedKey')
            if not last_evaluated_key:
                break  # Exit the loop when all records are processed

    except Exception as e:
        print(f"Error processing videos: {e}")
        logging.info(f"Error processing videos: {e}")

# Function to remove special characters and spaces from title
def sanitize_title(title):
    # Replace spaces and special characters with underscores
    sanitized_title = re.sub(r'[^A-Za-z0-9]+', '_', title)
    
    # Remove consecutive underscores
    sanitized_title = re.sub(r'_{2,}', '_', sanitized_title)
    
    # Remove trailing underscore if it exists
    if sanitized_title.endswith('_'):
        sanitized_title = sanitized_title.rstrip('_')
    
    return sanitized_title

# Function to write data to S3
def upload_to_s3_with_summary(title, video_url, transcript, summary):
    try:
        # Sanitize the title and prepare the file name
        sanitized_title = sanitize_title(title)

        video_id = video_url.split("v=")[1]
        if video_id.startswith('_'):
            video_id = video_id[1:]

        file_name = f"{sanitized_title}_{video_id}.txt"

        # Prepare the content for the file
        content = f"Title: {title}\n\nTranscript: {transcript}\n\nSummary: {summary}"

        # Upload the content to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=f"youtube_transcripts_with_summary/{file_name}",
            Body=content,
            ContentType='text/plain'
        )

        print(f"Successfully uploaded {file_name} to S3.")
    except Exception as e:
        print(f"Error uploading {file_name} to S3: {e}")

# Function to paginate through DynamoDB and process each item
def process_dynamodb_and_upload_with_summary():
    try:
        table = dynamodb.Table('youtube_video_data')

        # Initialize the scan with pagination
        last_evaluated_key = None

        while True:
            # Scan the DynamoDB table to retrieve video data
            if last_evaluated_key:
                response = table.scan(ExclusiveStartKey=last_evaluated_key)
            else:
                response = table.scan()

            # Loop through each item in the response
            for item in response['Items']:
                # Extract the necessary attributes from the item
                video_url = item.get('video_url')
                title = item.get('title')
                transcript = item.get('transcript', '')
                summary = item.get('summary', '')

                # Check if summary exists, and process
                if video_url and title and transcript:
                    # Write the data to S3
                    upload_to_s3_with_summary(title, video_url, transcript, summary)

            # If there's a LastEvaluatedKey, it means more records exist, so continue scanning
            last_evaluated_key = response.get('LastEvaluatedKey')
            if not last_evaluated_key:
                break  # Exit the loop when all records are processed

    except Exception as e:
        print(f"Error processing DynamoDB records: {e}")

if __name__ == "__main__":
    parameter = input("Enter the action that you need to perform: ")  # Get user input
    # parameter = "get_playlist_details"
    # parameter = "generate_summary"
    # parameter = "update_view_count"
    event_name = "re:Invent"
    event_year = "2024"
    
    if parameter == "generate_summary":
        logging.info(f"Inside if to call generate summary method")
        generate_summary()
    elif parameter == "upload_summary":
        process_dynamodb_and_upload_with_summary()
    elif parameter == "update_view_count":
        get_video_urls()
    elif parameter == "get_playlist_details":
        # Fetch and display videos
        print("Fetching AWS re:Invent 2024 videos...")
        videos = get_reinvent_2024_videos()

        print(f"\nTotal AWS re:Invent 2024 videos found: {len(videos)}")
        print("-" * 80)

        youtube_video_urls = []  # Initialize an empty list to store the video URLs

        for idx, video in enumerate(videos, 1):
            # Add the video URL to the youtube_video_urls list
            youtube_video_urls.append(video['url'])

        # playlist_id = "PL2yQDdvlhXf-5R7VtNr9P4nosA7DiDtM1"
        playlist_id = ""
        
        for youtube_video_url in youtube_video_urls:
            
            if not check_video_exists_in_dynamodb(youtube_video_url):
            # if check_video_exists_in_dynamodb(youtube_video_url):
                
                # Get video details (title, channel, upload date, duration)
                print(f"\n\nProcessing: {youtube_video_url}")
                video_details = get_video_details(youtube_video_url)
                if video_details:
                    title = video_details['title']
                    channel_name = video_details['channel_name']
                    upload_date = video_details['upload_date']
                    duration = video_details['duration']

                    # Fetch the transcript for the video
                    # transcript, transcript_sentences = process_youtube_url(youtube_video_url)

                    # Fetch the transcript for the video
                    result = process_youtube_url(youtube_video_url)

                    # Check if the result is not None before unpacking
                    if result is not None:
                        transcript, transcript_sentences = result

                        # Store the video data in DynamoDB
                        store_video_data_in_dynamodb(
                            playlist_url=f"https://www.youtube.com/playlist?list={playlist_id}",
                            video_url=youtube_video_url,
                            title=title,
                            event_name=event_name,
                            event_year=event_year,
                            # summary=summary,
                            channel_name=channel_name,
                            upload_date=upload_date,
                            duration=duration,
                            transcript=transcript,
                            transcript_sentences=transcript_sentences
                        )

                        #upload_to_s3(youtube_video_url, title, youtube_video_transcript, summary)
                        #upload_to_s3(youtube_video_url, title, youtube_video_transcript)
                else:
                    print(f"Skipping {youtube_video_url} due to missing details.")
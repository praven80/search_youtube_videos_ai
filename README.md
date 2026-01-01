# YouTube Data Processing Tool

A comprehensive Python tool for processing YouTube video data, including transcript extraction, summarization, and data storage using AWS services.

You can view a short demo video in the "Demo" folder.

## Features

- Extract video details and transcripts from YouTube videos
- Generate AI-powered summaries using AWS Bedrock
- Track video view counts
- Process YouTube playlists
- Store data in AWS DynamoDB and S3
- Support for various data processing operations
- Logging capabilities

## Prerequisites

- Python 3.8+
- AWS Account with access to:
  - AWS Bedrock
  - Amazon DynamoDB
  - Amazon S3
  - AWS IAM permissions
- Required Python packages (see requirements.txt)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd youtube-data-processor
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure AWS credentials:
```bash
aws configure
```

## Configuration

Set up the following environment variables or update the constants in the code:

```python
BEDROCK_MODEL_ID = "us.anthropic.claude-3-haiku-20240307-v1:0"
BEDROCK_MAX_TOKENS = 4096
BEDROCK_TEMPERATURE = 0.0
BEDROCK_TOP_P = 0.9
```

## Usage

The tool supports multiple operations that can be triggered using command-line arguments:

1. Generate Summary:
```bash
python main.py
Enter the action that you need to perform: generate_summary
```

2. Update View Count:
```bash
python main.py
Enter the action that you need to perform: update_view_count
```

3. Get Playlist Details:
```bash
python main.py
Enter the action that you need to perform: get_playlist_details
```

4. Upload Summary:
```bash
python main.py
Enter the action that you need to perform: upload_summary
```

## Features in Detail

### Video Processing
- Extract video metadata
- Download transcripts
- Process multiple videos concurrently
- Handle rate limiting and retries

### Data Storage
- DynamoDB for structured data
- S3 for transcript and summary storage
- Automatic data updates

### Analysis
- AI-powered content summarization
- Key points extraction
- AWS services identification
- Problem-solution mapping

## AWS Infrastructure

### DynamoDB Schema
```json
{
    "video_url": "string (primary key)",
    "title": "string",
    "event_name": "string",
    "event_year": "string",
    "channel_name": "string",
    "upload_date": "string",
    "duration": "string",
    "transcript": "string",
    "transcript_sentences": "string"
}
```

### S3 Structure
```
bucket/
├── youtube_transcripts/
└── youtube_transcripts_with_summary/
```

## Error Handling

- Automatic retries with exponential backoff
- Comprehensive error logging
- Failed operation tracking

## Logging

Logs are stored in `my_log_file.log` with the following format:
```
%(asctime)s - %(levelname)s - %(message)s
```
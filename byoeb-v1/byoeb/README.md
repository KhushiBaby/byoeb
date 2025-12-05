# Asha Saheli Bot

A conversational AI chatbot designed to assist Indian Community Health Workers (Accredited Social Health Activists or ASHAs) with their queries related to maternal health, child health, and rural health.

## Overview

Asha Saheli Bot is an intelligent assistant that helps ASHAs with their daily work by providing accurate, contextual responses to health-related queries. The bot supports multiple Indian languages including Hindi, Marathi, and Telugu, and can handle both text and audio inputs.

## Features

### Core Capabilities
- **Multi-language Support**: Supports many languages and allows addition of new ones easily.
- **Audio Processing**: Speech-to-text and text-to-speech capabilities
- **Contextual Responses**: Maintains conversation context for better responses
- **Expert Escalation**: Routes complex queries to human experts
- **Knowledge Base**: Access to comprehensive health information
- **WhatsApp Integration**: Primary communication channel

### Message Types Supported
- **Text Messages**: Direct text queries
- **Audio Messages**: Voice queries with automatic transcription
- **Interactive Messages**: Buttons and quick responses

### Response Features
- **Multi-language Output**: Responses in the user's preferred language
- **Follow-up Questions**: Suggests related questions for better engagement
- **Expert Verification**: Human expert review and verification system
- **Dead Letter Queue**: Failed message handling and retry mechanism

## Architecture

### Technology Stack Support
- **Backend**: Python with FastAPI
- **Message Queue**: Azure Storage Queue
- **Database**: Azure Cosmos DB (MongoDB API)
- **AI/ML**: Azure OpenAI (GPT-4o)
- **Speech Processing**: Azure Cognitive Services
- **Storage**: Azure Blob Storage
- **Monitoring**: Azure Application Insights
- **Vector Search**: Azure Cognitive Search

### System Components
```
byoeb/
├── chat_app/           # Main chat application
├── apis/              # API endpoints
├── services/          # Business logic services
├── factory/           # Factory patterns for clients
├── listener/          # Message queue consumers
├── background_jobs/   # Scheduled tasks
└── configuration/     # App configuration
```

## Installation & Setup

### Prerequisites
- Python 3.11+
- Poetry (dependency management)
- Azure account with required services
- WhatsApp Business API access

### Local Development Setup

1. **Clone the Repository**
   ```bash
   git clone <repository-url>
   cd byoeb-v1/byoeb
   ```

2. **Install Dependencies**
   ```bash
   # Install Poetry if not already installed
   curl -sSL https://install.python-poetry.org | python3 -
   
   # Install project dependencies
   poetry install
   ```

3. **Configure Environment Variables**
   
   Copy `keys.env.example` to `keys.env` and update with your credentials:
   ```bash
   cp keys.env.example keys.env
   ```

   Required environment variables:

    ```env
    # Application Environment
    APP_ENV=TEST
    
    # WhatsApp Configuration
    # Get from Meta Developer Console: https://developers.facebook.com/
    WHATSAPP_VERIFICATION_TOKEN=your_verification_token
    WHATSAPP_AUTH_TOKEN=your_auth_token
    WHATSAPP_PHONE_NUMBER_ID=your_phone_number_id
    
    # Azure OpenAI Configuration
    # Get from Azure Portal: OpenAI Service > Keys and Endpoint
    OPENAI_API_KEY=your_openai_api_key
    OPENAI_ORG_ID=your_org_id
    
    # Azure Storage Connection String
    # Get from Azure Portal: Storage Account > Access keys > Connection string
    # Required for local development (alternative to managed identity)
    AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=yourstorageaccount;AccountKey=yourstoragekey;EndpointSuffix=core.windows.net
    
    # MongoDB Connection String
    # Get from Azure Portal: Cosmos DB > Connection String
    MONGO_DB_CONNECTION_STRING=mongodb+srv://username:password@mongohostport/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000
    
    # Azure Application Insights
    # Get from Azure Portal: Application Insights > Connection strings
    APPINSIGHTS_CONNECTION_STRING=InstrumentationKey=your_instrumentation_key;IngestionEndpoint=https://your_region.applicationinsights.azure.com/;LiveEndpoint=https://your_region.livediagnostics.monitor.azure.com/;ApplicationId=your_application_id
    
    # OpenTelemetry Configuration (Optional)
    # Disable default instrumentation for specific packages
    OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=flask,django,psycopg2
    ```

4. **Local Development with Custom Integrations**
   
   To use local `byoeb-integrations` for development:
   ```bash
   # Edit pyproject.toml to use local path
   # Uncomment the local path line and comment the git line
   
   # Install with local integrations
   poetry install
   ```

5. **Run the Application**
   ```bash
   poetry run python byoeb/chat_app/run.py
   ```

## Configuration

### Azure Services Setup

1. **Azure Storage Account**
   - Create storage account for queues and blob storage
   - Configure access keys
   - Set up required queues: `botmessages`, `statusmessages`, `deadletterqueue`

2. **Azure Cosmos DB**
   - Create MongoDB API database
   - Set up collections: `ashausers`, `ashamessages`

3. **Azure OpenAI**
   - Deploy GPT-4o model
   - Configure speech-to-text and text-to-speech services

4. **Azure Cognitive Search**
   - Create search service for vector storage
   - Configure document index

### WhatsApp Business API

1. **Meta Developer Account**
   - Create WhatsApp Business app
   - Configure webhook endpoints
   - Set up message templates

2. **Webhook Configuration**
   - Endpoint: `/webhooks`
   - Verification token
   - Message handling

## Usage

### Starting the Application

```bash
# Development mode
poetry run python byoeb/chat_app/run.py

# Production mode
poetry run uvicorn byoeb.chat_app.run:app --host 0.0.0.0 --port 8000
```

### API Endpoints

The repository uses FAST API Swagger docs to keep API documentation up to date and readily available. Once the application is up and running, go to `{domain}/docs` to access the Swagger API documentation.

### 🔐 Authentication Options

This tool supports multiple authentication modes:

1. **Azure Managed Identity** (recommended for production in Azure)
2. **Azure CLI / Visual Studio** (recommended for local development)
3. **Environment Variables** (for local, CI/CD or non-Azure environments)

### Message Flow

1. **Message Reception**: WhatsApp webhook receives message
2. **Queue Processing**: Message added to Azure Storage Queue
3. **Message Consumer**: Processes messages from queue
4. **AI Processing**: OpenAI processes query and generates response
5. **Translation**: Response translated to user's language
6. **Delivery**: Response sent back via WhatsApp
7. **Expert Escalation**: Complex queries routed to human experts

## Development

### Project Structure

```
byoeb/
├── chat_app/
│   ├── configuration/     # App configuration and environment
│   ├── run.py            # Application entry point
│   └── app_config.json   # Application settings
├── apis/
│   ├── channel_register.py
│   └── background_jobs.py
├── services/
│   ├── chat/             # Chat processing services
│   └── databases/        # Database services
├── factory/
│   ├── channel.py        # Channel client factory
│   └── message_producer.py
├── listener/
│   └── message_consumer.py
└── background_jobs/
    └── daily_logs/       # Logging and monitoring
```

### Key Components

#### Message Consumer (`listener/message_consumer.py`)
- Processes messages from Azure Storage Queue
- Handles message retry logic
- Manages dead letter queue
- Integrates with chat services

#### Message Producer (`factory/message_producer.py`)
- Creates and manages queue clients
- Supports both status and bot message queues
- Handles connection string vs managed identity authentication

#### Chat Services (`services/chat/`)
- Message processing and response generation
- AI integration with OpenAI
- Translation services
- Expert escalation logic

### Local Development Tips

1. **Using Local Integrations**
   ```toml
   # In pyproject.toml, comment out git dependency and uncomment local path
   # byoeb-integrations = {git = "..."}
   byoeb-integrations = {path = "../byoeb-integrations", develop = true}
   ```

2. **Azure Storage Connection**
   - Use connection string for local development
   - Managed identity for production deployment
   - Configure proper authentication

3. **Environment Variables**
   - Use `keys.env` for local development
   - Set environment variables in production
   - Never commit sensitive credentials

## Troubleshooting

### Common Issues

1. **Azure Storage Authentication Error**
   - Verify connection string format
   - Check account key validity
   - Ensure proper permissions

2. **Message Queue Issues**
   - Check queue names in configuration
   - Verify queue permissions
   - Monitor dead letter queue

3. **OpenAI API Errors**
   - Verify API key and endpoint
   - Check rate limits
   - Validate model deployment

4. **WhatsApp Webhook Issues**
   - Verify webhook URL accessibility
   - Check verification token
   - Monitor webhook delivery

### Debug Mode

Enable debug logging by setting:
```env
LOG_LEVEL=DEBUG
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Support

For support and questions:
- Create an issue in the repository
- Contact the development team
- Check the documentation

**Note**: This bot is designed specifically for Indian Community Health Workers and should be used in accordance with local health guidelines and regulations.

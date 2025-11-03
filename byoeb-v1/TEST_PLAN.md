# BYOEB Staging Testing Plan
## Integration & Regression Test Suite

**Version:** 1.0  
**Last Updated:** 2025-11-03  
**Target Environment:** Staging  
**Production Components:** `byoeb-v1/{byoeb, byoeb-core, byoeb-integrations}`

---

## Table of Contents
1. [Overview](#overview)
2. [Test Environment Setup](#test-environment-setup)
3. [Integration Test Suite](#integration-test-suite)
4. [Regression Test Suite](#regression-test-suite)
5. [Performance & Load Tests](#performance--load-tests)
6. [Test Execution Strategy](#test-execution-strategy)
7. [Success Criteria](#success-criteria)
8. [Rollback Procedures](#rollback-procedures)

---

## Overview

### Purpose
This test plan defines comprehensive integration and regression testing procedures to be executed after staging build deployment for the BYOEB (Build Your Own Expert Bot) platform. The system is an expert-in-the-loop WhatsApp chatbot for Indian Community Health Workers (ASHAs).

### Scope
- **In Scope:** All production components in `byoeb-v1/byoeb` including APIs, services, message consumers, and background jobs
- **Out of Scope:** Legacy components in root directory (old Flask-based system)

### Testing Approach
- **Integration Tests:** Validate component interactions and end-to-end workflows
- **Regression Tests:** Ensure existing functionality remains intact after changes
- **Performance Tests:** Validate system behavior under load
- **Data Integrity Tests:** Verify database operations and data consistency

---

## Test Environment Setup

### Prerequisites

#### 1. Environment Variables
Ensure all required environment variables are configured in staging:

```bash
# Application
APP_ENV=STAGING

# WhatsApp
WHATSAPP_VERIFICATION_TOKEN=<staging_token>
WHATSAPP_AUTH_TOKEN=<staging_auth>
WHATSAPP_PHONE_NUMBER_ID=<staging_phone_id>

# Azure OpenAI
OPENAI_API_KEY=<staging_key>
OPENAI_ORG_ID=<staging_org>

# Azure Storage
AZURE_STORAGE_CONNECTION_STRING=<staging_connection_string>

# MongoDB/Cosmos DB
MONGO_DB_CONNECTION_STRING=<staging_mongo_connection>

# Application Insights
APPINSIGHTS_CONNECTION_STRING=<staging_insights>
```

#### 2. Test Data Setup
```bash
# Set test user credentials
export PHONE_NUMBER_ID=<test_whatsapp_number>
export USER_NAME="Test User"
export RECIEVE_URL=<staging_base_url>/receive
```

#### 3. Database Preparation
- Create test collections in staging MongoDB
- Seed initial test data (users, knowledge base entries)
- Configure test WhatsApp phone numbers
- Set up test expert accounts

#### 4. Azure Resources Verification
```bash
# Verify queues exist
- botmessages
- statusmessages
- deadletterqueue

# Verify blob containers
- Check audio/media storage containers

# Verify Cosmos DB collections
- ashausers
- ashamessages
```

---

## Integration Test Suite

### 1. WhatsApp Webhook Integration

#### Test 1.1: Webhook Verification
**Objective:** Verify WhatsApp webhook registration endpoint

**Endpoint:** `GET /receive`

**Test Steps:**
```bash
curl -X GET "${RECIEVE_URL}?hub.mode=subscribe&hub.verify_token=${WHATSAPP_VERIFICATION_TOKEN}&hub.challenge=test_challenge_123"
```

**Expected Result:**
- Status: 200
- Response: `test_challenge_123`

**Validation:**
- Webhook verification token validated correctly
- Challenge response returned properly

---

#### Test 1.2: Incoming Text Message Reception
**Objective:** Verify system can receive and queue text messages

**Test Payload:**
```json
{
  "object": "whatsapp_business_account",
  "entry": [{
    "id": "test_entry_id",
    "changes": [{
      "value": {
        "messaging_product": "whatsapp",
        "metadata": {
          "display_phone_number": "919001386867",
          "phone_number_id": "test_phone_id"
        },
        "contacts": [{
          "profile": {"name": "Test User"},
          "wa_id": "test_user_wa_id"
        }],
        "messages": [{
          "from": "test_user_wa_id",
          "id": "test_message_id",
          "timestamp": "1234567890",
          "text": {"body": "Test message"},
          "type": "text"
        }]
      },
      "field": "messages"
    }]
  }]
}
```

**Expected Result:**
- Status: 200
- Message queued to `botmessages` Azure Storage Queue
- Message visible in queue within 5 seconds

**Validation:**
```python
# Check queue contains message
queue_client.peek_messages(max_messages=1)
# Verify message_id matches test_message_id
```

---

#### Test 1.3: Audio Message Reception
**Objective:** Verify audio message handling and media download

**Test Steps:**
1. Send audio message webhook payload
2. Verify audio file downloaded to Azure Blob Storage
3. Check speech-to-text transcription initiated

**Expected Result:**
- Audio file stored in blob storage
- Transcription job initiated
- Message queued for processing

---

#### Test 1.4: Interactive Message (Buttons/Lists)
**Objective:** Verify interactive message handling

**Test Types:**
- Button replies
- List replies
- Quick replies

**Expected Result:**
- Interactive responses parsed correctly
- Context maintained from previous message
- Appropriate response generated

---

### 2. Message Queue Processing

#### Test 2.1: Message Consumer Initialization
**Objective:** Verify message consumer starts and connects to queue

**Test Steps:**
```python
# Start message consumer
consumer = QueueConsumer(
    account_url=AZURE_STORAGE_URL,
    queue_name="botmessages",
    config=config,
    user_db_service=user_db,
    message_db_service=message_db,
    channel_client_factory=channel_factory,
    consumer_type="azure_storage_queue"
)
await consumer.initialize()
```

**Expected Result:**
- Consumer initializes without errors
- Connection to Azure Storage Queue established
- Logging confirms initialization

**Validation:**
- Check logs for "Azure storage queue client created"
- Verify no connection errors

---

#### Test 2.2: Batch Message Processing
**Objective:** Verify batch processing of multiple messages

**Test Steps:**
1. Queue 10 messages simultaneously
2. Monitor consumer processing
3. Verify all messages processed

**Expected Result:**
- All messages processed within 60 seconds
- No message loss
- Messages deleted from queue after processing
- Dead letter queue empty

**Validation:**
```python
# Check processed count
assert len(successfully_processed_messages) == 10
# Verify queue empty
assert queue_client.get_queue_properties().approximate_message_count == 0
```

---

#### Test 2.3: Dead Letter Queue Handling
**Objective:** Verify messages exceeding retry count move to DLQ

**Test Steps:**
1. Queue a message that will fail processing
2. Let it retry according to `queue_retry_count`
3. Verify message moves to dead letter queue

**Expected Result:**
- Message retried `queue_retry_count` times
- Message moved to `deadletterqueue` after max retries
- Original queue cleaned up

**Validation:**
- Check DLQ contains the failed message
- Verify dequeue_count exceeded threshold

---

#### Test 2.4: Duplicate Message Detection
**Objective:** Ensure duplicate messages are not processed twice

**Test Steps:**
1. Send same message twice with identical message_id
2. Verify only processed once

**Expected Result:**
- First message processed successfully
- Second message skipped with log "Message already processed"
- Database contains only one record

---

### 3. User Flow Integration Tests

#### Test 3.1: New User Onboarding Flow (End-to-End)
**Objective:** Verify complete onboarding workflow for new users

**Test Scenario:** Based on existing `test_onboarding.py`

**Flow Steps:**
1. **Initial Contact:** User sends "hi"
2. **Language Selection:** User selects preferred language
3. **Role Selection:** User identifies role (ASHA/Expert)
4. **Confirmation:** User confirms participation
5. **First Query:** User asks a health-related question

**Test Implementation:**
```python
def test_complete_onboarding_flow():
    # Step 1: Send initial "hi"
    response = send_whatsapp_message(payload_1)
    assert response.status_code == 200
    
    # Wait for language selection message
    bot_messages = get_bot_messages_after(timestamp_1)
    language_message = find_message_with_keyword(bot_messages, "language")
    assert language_message is not None
    
    # Step 2: Select language (English)
    response = send_language_selection("English")
    assert response.status_code == 200
    
    # Wait for role selection
    bot_messages = get_bot_messages_after(timestamp_2)
    role_message = find_message_with_keyword(bot_messages, "Who are you")
    assert role_message is not None
    
    # Step 3: Select role (Others/ASHA)
    response = send_role_selection("Others")
    assert response.status_code == 200
    
    # Step 4: Confirm participation
    response = send_confirmation("Yes")
    assert response.status_code == 200
    
    # Step 5: Send first query
    response = send_text_query("What is antara injection?")
    assert response.status_code == 200
    
    # Verify user created in database
    user = get_user_by_phone(PHONE_NUMBER_ID)
    assert user is not None
    assert user["user_type"] == "Asha"
    assert user["user_language"] == "en"
```

**Expected Result:**
- User successfully onboarded
- User record created in `ashausers` collection
- Language preference stored
- User type correctly assigned
- First query processed

**Validation Queries:**
```python
# Database validation
user = user_db.get_from_whatsapp_id(PHONE_NUMBER_ID)
assert user["onboarding_complete"] == True
assert user["user_language"] == "en"

# Message validation
messages = message_db.get_user_messages(user["user_id"])
assert len(messages) >= 5  # Onboarding messages + first query
```

---

#### Test 3.2: Returning User Message Flow
**Objective:** Verify returning users can send queries without re-onboarding

**Test Steps:**
1. Ensure user exists in database
2. Send query message
3. Verify query processed without onboarding

**Expected Result:**
- No onboarding messages sent
- Query processed immediately
- Context from previous conversations loaded

---

#### Test 3.3: Language Preference Handling
**Objective:** Verify messages translated to user's preferred language

**Test Languages:**
- Hindi
- English
- Marathi
- Telugu

**Test Steps:**
1. Set user language preference
2. Send English query
3. Verify response in user's language

**Expected Result:**
- Query translated from source language to English
- AI response generated in English
- Response translated to user's preferred language
- User receives message in their language

---

### 4. Knowledge Base & AI Integration

#### Test 4.1: RAG Query Processing
**Objective:** Verify Retrieval-Augmented Generation (RAG) pipeline

**Test Steps:**
1. Send health-related query
2. Verify vector search executed
3. Check relevant chunks retrieved
4. Validate AI response generated
5. Verify citations included

**Test Query:** "What are the side effects of Antara injection?"

**Expected Result:**
- Vector search returns relevant chunks (top_k=3 or 7)
- Chunks from knowledge base retrieved
- GPT-4o generates response using chunks
- Response includes citations if `SHOW_CITATIONS=true`
- Query type classified (e.g., "Clinical")

**Validation:**
```python
# Check knowledge base query
chunks = vector_store.query("side effects Antara injection", top_k=3)
assert len(chunks) > 0

# Check AI response
response = knowledge_base.answer_query(user_conv_db, bot_conv_db, msg_id, app_logger)
assert "Antara" in response.gpt_output
assert response.query_type in ["Clinical", "Administrative", "small-talk"]
```

---

#### Test 4.2: Conversation Context Handling
**Objective:** Verify system maintains conversation history

**Test Steps:**
1. Send initial query
2. Send follow-up query referencing previous response
3. Verify context used in generating response

**Expected Result:**
- Previous 2-3 conversations loaded
- Context included in prompt to AI
- Response considers conversation history

---

#### Test 4.3: Small Talk Detection
**Objective:** Verify system handles non-clinical queries

**Test Queries:**
- "Hello, how are you?"
- "Thank you"
- "Goodbye"

**Expected Result:**
- Query classified as "small-talk"
- Appropriate response generated
- Not escalated to experts

---

#### Test 4.4: Unknown Query Handling
**Objective:** Verify handling when knowledge base has no answer

**Test Steps:**
1. Send query outside knowledge base domain
2. Verify fallback response

**Expected Result:**
- Response: "I do not know the answer to your question"
- Query escalated to expert (if configured)
- User notified appropriately

---

### 5. Expert Escalation Flow

#### Test 5.1: Query Escalation to Expert
**Objective:** Verify queries escalate to human experts

**Test Steps:**
1. Send clinical query requiring expert verification
2. Verify AI generates draft response
3. Check expert notification sent
4. Verify expert can approve/edit response

**Expected Result:**
- Draft response generated
- Expert receives notification via WhatsApp
- Expert can view query and draft response
- Expert can approve, edit, or escalate

**Validation:**
```python
# Check expert conversation record
expert_conv = expert_conv_db.find_pending_by_user_message(msg_id)
assert expert_conv is not None
assert expert_conv["status"] == "pending_expert_review"
```

---

#### Test 5.2: Multiple Expert Escalation
**Objective:** Verify consensus mechanism for complex queries

**Test Steps:**
1. Send query requiring multiple expert opinions
2. Verify query sent to N experts (per `NUM_ESCALATE_EXPERTS`)
3. Collect expert responses
4. Verify consensus mechanism runs

**Expected Result:**
- Query sent to multiple experts
- Minimum `MIN_CONSENSUS_RESPONSES` collected
- Consensus response generated
- Final response sent to user

---

#### Test 5.3: Expert Response Approval
**Objective:** Verify expert-approved responses sent to users

**Test Steps:**
1. Expert receives query
2. Expert sends approved response
3. Verify response delivered to user
4. Check knowledge base update if response edited

**Expected Result:**
- User receives expert-approved response
- Response stored in bot_conversations
- If edited, correction queued for KB update

---

### 6. Background Jobs Integration

#### Test 6.1: Consensus Job Execution
**Objective:** Verify consensus background job processes pending queries

**Cron Schedule:** `*/30 * * * *` (Every 30 minutes)

**Test Steps:**
1. Create queries awaiting consensus
2. Trigger consensus job
3. Verify consensus calculated
4. Check responses sent

**Expected Result:**
- Job finds pending consensus queries
- Processes queries with sufficient responses
- Sends consensus responses to users

---

#### Test 6.2: Daily Logs Compilation
**Objective:** Verify daily usage statistics compiled

**Cron Schedule:** `30 16 * * 1-6`

**Test Steps:**
1. Generate test activity (queries, responses)
2. Trigger daily logs job
3. Verify logs compiled

**Expected Result:**
- Usage statistics calculated
- Logs stored in database
- Reports generated (if configured)

---

#### Test 6.3: Did You Know (DYK) Messages
**Objective:** Verify educational messages sent to users

**Cron Schedule:** `00 12 * * 5`

**Test Steps:**
1. Trigger DYK job
2. Verify users receive educational content
3. Check suggested follow-up questions

**Expected Result:**
- DYK message sent to active users
- Content from configured facts
- Follow-up questions suggested

---

#### Test 6.4: Leaderboard Generation
**Objective:** Verify expert leaderboard updates

**Test Steps:**
1. Generate expert activity
2. Trigger leaderboard job
3. Verify rankings calculated

**Expected Result:**
- Expert activity counted
- Leaderboard updated
- Top experts identified

---

### 7. Database Integration

#### Test 7.1: User Database Operations
**Objective:** Verify user CRUD operations

**Test Operations:**
```python
# Create
user_id = user_db.insert_row(
    user_id=str(uuid4()),
    whatsapp_id="test_wa_id",
    user_type="Asha",
    user_language="en",
    test_user=True
)

# Read
user = user_db.get_from_whatsapp_id("test_wa_id")
assert user is not None

# Update
user_db.update_user_language(user["user_id"], "hi")

# Verify update
user = user_db.get_from_user_id(user["user_id"])
assert user["user_language"] == "hi"

# Delete
user_db.delete_user_by_wa_id("test_wa_id")
```

**Expected Result:**
- All CRUD operations succeed
- Data consistency maintained
- Cache invalidation works correctly

---

#### Test 7.2: Message Database Operations
**Objective:** Verify message storage and retrieval

**Test Operations:**
- Store user message
- Store bot response
- Retrieve conversation history
- Query messages by user_id
- Query messages by message_id

**Expected Result:**
- Messages stored with correct timestamps
- Relationships maintained (user-bot message pairs)
- Queries return correct results

---

#### Test 7.3: Database Transaction Consistency
**Objective:** Verify data consistency across collections

**Test Scenario:**
1. Create user
2. Store user message
3. Generate bot response
4. Store bot response
5. Link message records

**Expected Result:**
- All records created
- References correct (user_id, message_id)
- No orphaned records
- Timestamps consistent

---

### 8. API Endpoint Integration

#### Test 8.1: Health Check Endpoint
**Objective:** Verify health check returns system status

**Endpoint:** `GET /health`

**Expected Result:**
```json
{
  "status": "healthy",
  "timestamp": "2025-11-03T14:00:00Z",
  "services": {
    "database": "up",
    "message_queue": "up",
    "vector_store": "up"
  }
}
```

---

#### Test 8.2: User Management APIs
**Objective:** Verify user CRUD APIs

**Endpoints:**
- `GET /get_users` - Retrieve users
- `DELETE /delete_users` - Delete users
- `POST /admin/users` - Create/update users

**Test Steps:**
1. Create test user via API
2. Retrieve user details
3. Update user properties
4. Delete user

**Expected Result:**
- All operations succeed
- Proper authentication/authorization
- Data validation enforced

---

#### Test 8.3: Message Retrieval API
**Objective:** Verify message history retrieval

**Endpoint:** `GET /get_bot_messages?timestamp={timestamp}`

**Test Steps:**
1. Send messages with known timestamps
2. Query messages after specific timestamp
3. Verify correct messages returned

**Expected Result:**
- Messages filtered by timestamp correctly
- Response includes all required fields
- Pagination works (if implemented)

---

#### Test 8.4: Knowledge Base Update API
**Objective:** Verify KB update workflow via API

**Endpoints:**
- `GET /process_response_to_send_for_kb_update`
- `GET /process_expert_responses_to_update_kb`

**Test Steps:**
1. Generate expert corrections
2. Trigger KB update process
3. Verify updates applied to vector store

**Expected Result:**
- Corrections extracted
- Vector store updated
- New embeddings created

---

### 9. Translation & Speech Services

#### Test 9.1: Text Translation
**Objective:** Verify Azure Translator integration

**Test Cases:**
- English → Hindi
- Hindi → English
- English → Marathi
- English → Telugu

**Expected Result:**
- Translations accurate
- Language codes correct
- API calls succeed
- Fallback handling for unsupported languages

---

#### Test 9.2: Speech-to-Text
**Objective:** Verify audio message transcription

**Test Steps:**
1. Send audio message
2. Verify transcription initiated
3. Check transcribed text stored

**Expected Result:**
- Audio downloaded
- Speech-to-text API called
- Transcription stored in message record
- Language detected correctly

---

#### Test 9.3: Text-to-Speech
**Objective:** Verify audio response generation

**Test Steps:**
1. Generate text response
2. Trigger TTS conversion
3. Verify audio file created
4. Check audio sent to user

**Expected Result:**
- TTS conversion succeeds
- Audio stored in blob storage
- Audio URL sent via WhatsApp
- Audio file playable

---

### 10. Error Handling & Resilience

#### Test 10.1: API Rate Limiting
**Objective:** Verify system handles rate limits gracefully

**Test Steps:**
1. Send rapid burst of requests
2. Monitor for rate limit errors
3. Verify retry logic engages

**Expected Result:**
- Rate limits detected
- Exponential backoff implemented
- Requests eventually succeed
- No data loss

---

#### Test 10.2: Azure Service Outage Simulation
**Objective:** Verify graceful degradation

**Test Scenarios:**
- Azure Storage unavailable
- Cosmos DB connection failure
- OpenAI API timeout

**Expected Result:**
- Errors logged appropriately
- User receives error message (not crash)
- Retry mechanisms engage
- System recovers when services restore

---

#### Test 10.3: Malformed Message Handling
**Objective:** Verify system handles invalid payloads

**Test Cases:**
- Missing required fields
- Invalid JSON
- Unknown message types
- Empty messages

**Expected Result:**
- Validation errors caught
- Messages logged
- No system crash
- DLQ used appropriately

---

#### Test 10.4: Database Connection Resilience
**Objective:** Verify connection pool and retry logic

**Test Steps:**
1. Simulate connection timeout
2. Verify retry logic
3. Check connection pool recovery

**Expected Result:**
- Connection retries attempted
- Pool recovers after transient failures
- Operations eventually succeed
- No connection leaks

---

## Regression Test Suite

### Purpose
Ensure existing functionality remains intact after code changes, deployments, or configuration updates.

---

### 1. Core Functionality Regression

#### Regression 1.1: Message Flow Integrity
**Objective:** Verify complete message flow still works

**Test Coverage:**
- User sends text message
- Bot generates response
- Response delivered to user
- All database records created

**Reference Baseline:** Previous production logs/metrics

**Pass Criteria:**
- 100% message delivery rate
- Average response time within baseline ± 20%
- No increase in error rates

---

#### Regression 1.2: User Authentication & Sessions
**Objective:** Verify user identification works

**Test Cases:**
- Existing user recognized
- New user created
- User preferences persisted
- Session data maintained

**Pass Criteria:**
- All existing users load correctly
- No duplicate user records created
- Cache hit rate maintained

---

#### Regression 1.3: Language Support
**Objective:** Verify all supported languages work

**Test Matrix:**
| Language | Input | Translation | Response | TTS |
|----------|-------|-------------|----------|-----|
| Hindi    | ✓     | ✓           | ✓        | ✓   |
| English  | ✓     | ✓           | ✓        | ✓   |
| Marathi  | ✓     | ✓           | ✓        | ✓   |
| Telugu   | ✓     | ✓           | ✓        | ✓   |

**Pass Criteria:**
- All languages functional
- Translation quality maintained
- No regression in accuracy

---

### 2. Data Integrity Regression

#### Regression 2.1: Database Schema Consistency
**Objective:** Verify no breaking changes to data models

**Test Steps:**
1. Query sample records from each collection
2. Verify all expected fields present
3. Check data types correct

**Collections to Validate:**
- ashausers
- ashamessages
- expert_conversations
- user_conversations

**Pass Criteria:**
- All fields accessible
- No data corruption
- Indexes functional

---

#### Regression 2.2: Historical Data Accessibility
**Objective:** Verify old messages/users still accessible

**Test Steps:**
1. Query messages from before deployment
2. Retrieve user records created previously
3. Verify conversation history loads

**Pass Criteria:**
- All historical data accessible
- No data loss
- Queries perform within acceptable time

---

### 3. API Backward Compatibility

#### Regression 3.1: API Contract Validation
**Objective:** Ensure APIs maintain contracts

**Test Approach:**
1. Use previous API test suite
2. Run against new deployment
3. Verify responses match schema

**Pass Criteria:**
- All endpoints respond
- Response schemas unchanged (or backward compatible)
- Status codes consistent

---

#### Regression 3.2: Webhook Payload Compatibility
**Objective:** Verify webhook handlers accept previous payload formats

**Test Steps:**
1. Send archived webhook payloads
2. Verify processed correctly
3. Check responses generated

**Pass Criteria:**
- Old payloads processed successfully
- No parsing errors
- Responses appropriate

---

### 4. Performance Regression

#### Regression 4.1: Response Time Baseline
**Objective:** Ensure response times not degraded

**Metrics to Track:**
| Operation              | Baseline | Threshold | Current |
|------------------------|----------|-----------|---------|
| Message Receipt        | 50ms     | 75ms      |         |
| Queue Processing       | 2s       | 3s        |         |
| AI Response Generation | 5s       | 7s        |         |
| Message Delivery       | 1s       | 1.5s      |         |
| End-to-End             | 10s      | 15s       |         |

**Pass Criteria:**
- No metric exceeds threshold
- 95th percentile within baseline ± 20%

---

#### Regression 4.2: Throughput Validation
**Objective:** Verify system handles expected load

**Test Parameters:**
- Messages per minute: 100
- Concurrent users: 50
- Test duration: 30 minutes

**Pass Criteria:**
- All messages processed
- No queue backlog
- Error rate < 0.1%

---

### 5. Configuration Regression

#### Regression 5.1: Feature Flags
**Objective:** Verify feature toggles work

**Features to Test:**
- `API_ACTIVATED`
- `SHOW_CITATIONS`
- `SEND_POLL`
- `SUGGEST_NEXT_QUESTIONS`
- `ESCALATE_MULTIPLE`
- `SEND_FEEDBACK_POLL`

**Test Approach:**
For each feature:
1. Enable feature
2. Verify behavior
3. Disable feature
4. Verify disabled behavior

**Pass Criteria:**
- All flags functional
- Behavior changes as expected
- No side effects

---

#### Regression 5.2: Environment-Specific Config
**Objective:** Verify staging config different from production

**Checks:**
- Database connections point to staging
- Message queues use staging resources
- WhatsApp uses test phone numbers
- No production API keys used

**Pass Criteria:**
- Complete isolation from production
- No cross-environment contamination

---

### 6. Security Regression

#### Regression 6.1: Authentication & Authorization
**Objective:** Verify security controls intact

**Test Cases:**
- Unauthorized API access blocked
- Invalid tokens rejected
- Webhook verification enforced
- User data access restricted

**Pass Criteria:**
- No security bypasses
- Authorization checks enforced
- Audit logs generated

---

#### Regression 6.2: Data Privacy
**Objective:** Ensure sensitive data protected

**Checks:**
- PII not logged in plain text
- Secrets not exposed in responses
- User data isolated per user

**Pass Criteria:**
- No PII in application logs
- No secrets in error messages
- User data access validated

---

### 7. Integration Points Regression

#### Regression 7.1: WhatsApp API Integration
**Objective:** Verify WhatsApp Business API integration intact

**Test Cases:**
- Send text message
- Send audio message
- Send interactive message
- Send media message
- Receive status updates

**Pass Criteria:**
- All message types work
- Status updates processed
- Media handling functional

---

#### Regression 7.2: Azure Services Integration
**Objective:** Verify all Azure service integrations

**Services to Test:**
- Azure Storage Queue
- Azure Blob Storage
- Cosmos DB (MongoDB API)
- Azure OpenAI
- Azure Cognitive Services (Speech, Translation)
- Azure Cognitive Search
- Application Insights

**Pass Criteria:**
- All services accessible
- Operations succeed
- Monitoring/telemetry working

---

### 8. Background Job Regression

#### Regression 8.1: Scheduled Job Execution
**Objective:** Verify all cron jobs execute

**Jobs to Test:**
- `send_query_to_expert.py` - Every 30 min
- `respond_with_consensus.py` - Every 30 min
- `send_dyk.py` - Weekly (Friday 12:00)
- `asha_logs.py` - Daily (16:30, Mon-Sat)
- `leaderboard.py` - Weekly

**Test Approach:**
1. Manually trigger each job
2. Verify execution completes
3. Check expected outputs

**Pass Criteria:**
- All jobs execute successfully
- Outputs correct
- No errors logged

---

#### Regression 8.2: Job Dependencies
**Objective:** Verify jobs don't interfere with each other

**Test Scenario:**
- Run multiple jobs simultaneously
- Check for race conditions
- Verify data consistency

**Pass Criteria:**
- No deadlocks
- No data corruption
- Jobs complete successfully

---

## Performance & Load Tests

### Load Test 1: Message Throughput
**Objective:** Validate system handles expected message volume

**Test Configuration:**
- Concurrent Users: 100
- Messages per User: 10
- Total Messages: 1,000
- Duration: 10 minutes

**Success Criteria:**
- All messages processed
- Average response time < 10s
- Error rate < 0.5%
- No queue backlog

---

### Load Test 2: Burst Traffic
**Objective:** Validate system handles traffic spikes

**Test Configuration:**
- Burst 500 messages in 30 seconds
- Monitor queue depth
- Track processing time

**Success Criteria:**
- Queue absorbs burst
- Processing catches up within 5 minutes
- No message loss
- No service degradation

---

### Load Test 3: Sustained Load
**Objective:** Validate system stability under continuous load

**Test Configuration:**
- Duration: 2 hours
- Constant rate: 50 messages/minute
- Total messages: 6,000

**Success Criteria:**
- No memory leaks
- Response times stable
- Error rate < 0.1%
- All messages processed

---

### Load Test 4: Expert Escalation Scale
**Objective:** Validate expert notification system under load

**Test Configuration:**
- 100 queries requiring escalation
- 10 active experts
- Multiple consensus queries

**Success Criteria:**
- All experts notified
- Consensus calculated correctly
- No notification failures
- Response times acceptable

---

### Stress Test: Resource Limits
**Objective:** Identify system breaking point

**Test Approach:**
- Gradually increase load
- Monitor resource utilization (CPU, memory, connections)
- Identify bottlenecks

**Metrics to Collect:**
- Maximum throughput achieved
- Resource utilization at limit
- Failure modes
- Recovery behavior

---

## Test Execution Strategy

### Pre-Deployment Checklist

**1. Environment Preparation**
- [ ] Staging environment provisioned
- [ ] All Azure resources created
- [ ] Environment variables configured
- [ ] Test data seeded
- [ ] Test WhatsApp numbers registered

**2. Deployment Validation**
- [ ] Application deployed successfully
- [ ] Health check endpoint returns 200
- [ ] Message consumer running
- [ ] Logs flowing to Application Insights
- [ ] Database connections established

**3. Smoke Tests**
- [ ] Send test message → receive response
- [ ] User onboarding flow completes
- [ ] Database write/read operations work
- [ ] Background job triggers manually

---

### Test Execution Order

**Phase 1: Smoke Tests (15 minutes)**
1. Health check
2. Simple message flow
3. Database connectivity
4. Basic API calls

**Phase 2: Integration Tests (2 hours)**
1. Webhook integration (30 min)
2. Message queue processing (30 min)
3. User flows (30 min)
4. Knowledge base & AI (30 min)

**Phase 3: Regression Tests (3 hours)**
1. Core functionality (1 hour)
2. Data integrity (30 min)
3. API compatibility (30 min)
4. Performance baselines (1 hour)

**Phase 4: Load & Performance Tests (2 hours)**
1. Throughput test (30 min)
2. Burst traffic test (30 min)
3. Sustained load test (1 hour)

**Phase 5: End-to-End Validation (1 hour)**
1. Complete user journey
2. Expert escalation flow
3. Background job execution
4. Monitoring & alerting verification

**Total Estimated Time: 8-9 hours**

---

### Automation Strategy

**Test Framework:** pytest

**Automated Test Categories:**
1. **Unit Tests** - Already in codebase
2. **Integration Tests** - New (this plan)
3. **Regression Tests** - Automated subset
4. **Load Tests** - Locust or Artillery

**CI/CD Integration:**
```yaml
# Example GitHub Actions workflow
name: Staging Tests
on:
  deployment_status:
    types: [success]

jobs:
  smoke-tests:
    runs-on: ubuntu-latest
    steps:
      - name: Run Smoke Tests
        run: pytest tests/smoke/ --env=staging
  
  integration-tests:
    needs: smoke-tests
    runs-on: ubuntu-latest
    steps:
      - name: Run Integration Tests
        run: pytest tests/integration/ --env=staging
  
  regression-tests:
    needs: integration-tests
    runs-on: ubuntu-latest
    steps:
      - name: Run Regression Tests
        run: pytest tests/regression/ --env=staging
```

---

### Manual Test Procedures

**Manual Test Checklist:**
- [ ] WhatsApp message sending (real device)
- [ ] Audio message recording and playback
- [ ] Interactive button clicks
- [ ] Multi-language response verification
- [ ] Expert review interface (if available)
- [ ] Visual inspection of responses

**Test Devices:**
- iOS device with WhatsApp
- Android device with WhatsApp
- WhatsApp Business API sandbox

---

## Success Criteria

### Critical Success Factors

**All tests must pass:**
1. ✅ Smoke tests (100% pass rate)
2. ✅ Core user flows (100% pass rate)
3. ✅ API integration tests (95% pass rate)
4. ✅ Data integrity checks (100% pass rate)

**Performance within bounds:**
- Average message response time < 10 seconds
- Message delivery success rate > 99.5%
- Queue processing lag < 30 seconds
- Error rate < 0.5%

**No regressions:**
- Zero critical bugs introduced
- No data loss
- No security vulnerabilities
- Performance not degraded > 20%

---

### Go/No-Go Decision Criteria

**GO Criteria:**
- All critical tests pass
- Performance within acceptable range
- No blocking bugs
- Stakeholder approval

**NO-GO Criteria:**
- Any critical test fails
- Data loss detected
- Security vulnerability found
- Performance degraded > 30%
- Rollback procedure required

---

## Rollback Procedures

### Rollback Triggers

**Immediate Rollback Required:**
- Complete system failure
- Data loss or corruption
- Security breach
- Unable to process any messages

**Considered Rollback:**
- Error rate > 5%
- Performance degradation > 50%
- Multiple integration failures
- Significant functionality broken

---

### Rollback Steps

**1. Stop Current Deployment**
```bash
# Scale down current deployment
az webapp stop --name byoeb-staging --resource-group byoeb-rg
```

**2. Restore Previous Version**
```bash
# Deploy previous stable version
az webapp deployment source config-zip \
  --resource-group byoeb-rg \
  --name byoeb-staging \
  --src previous-stable-build.zip
```

**3. Verify Services**
```bash
# Health check
curl https://byoeb-staging.azurewebsites.net/health

# Smoke test
pytest tests/smoke/ --env=staging
```

**4. Clear Queues (if necessary)**
```bash
# Clear stuck messages from queue
az storage queue clear \
  --name botmessages \
  --connection-string $AZURE_STORAGE_CONNECTION_STRING
```

**5. Notify Stakeholders**
- Send rollback notification
- Document issues encountered
- Schedule post-mortem

**6. Database Rollback (if needed)**
```bash
# Restore database snapshot
# Only if schema changes introduced
az cosmosdb mongodb database restore \
  --account-name byoeb-cosmos \
  --resource-group byoeb-rg \
  --name byoeb-db \
  --restore-timestamp <timestamp>
```

---

## Test Reporting

### Test Execution Report Template

```markdown
# BYOEB Staging Test Report
**Date:** YYYY-MM-DD
**Build Version:** vX.X.X
**Environment:** Staging
**Tester:** [Name]

## Summary
- Total Tests: X
- Passed: X (XX%)
- Failed: X (XX%)
- Skipped: X (XX%)

## Test Results by Category

### Integration Tests
- Passed: X/Y
- Failed: Z
- Critical Failures: None

### Regression Tests
- Passed: X/Y
- Failed: Z
- Regressions Found: None

### Performance Tests
- Throughput: X msg/min (Target: Y)
- Avg Response Time: Xs (Target: Ys)
- Error Rate: X% (Target: <0.5%)

## Critical Issues
1. [Issue description]
   - Severity: Critical/High/Medium/Low
   - Impact: [Description]
   - Workaround: [If available]

## Recommendations
- [ ] Deploy to production
- [ ] Address issues and retest
- [ ] Rollback to previous version

## Next Steps
[Action items]
```

---

### Metrics Dashboard

**Key Metrics to Monitor:**
1. Test pass rate (%)
2. Response time (p50, p95, p99)
3. Error rate (%)
4. Message throughput (msg/min)
5. Queue depth
6. Database query latency
7. API success rate
8. Expert escalation rate

**Tools:**
- Azure Application Insights
- Custom Grafana dashboards
- Test execution reports (pytest-html)

---

## Continuous Improvement

### Test Plan Maintenance

**Review Frequency:** Monthly

**Update Triggers:**
- New feature additions
- Bug fixes
- Performance optimizations
- Infrastructure changes
- Security updates

**Version Control:**
- Test plan versioned with code
- Changes reviewed in PR process
- Test coverage tracked over time

---

### Test Case Expansion

**Areas for Additional Coverage:**
1. Edge cases for user inputs
2. Network failure scenarios
3. Concurrent user operations
4. Long-running conversation threads
5. Knowledge base updates at scale
6. Multi-expert consensus edge cases

---

## Appendix

### A. Test Data Templates

**Sample User:**
```json
{
  "user_id": "test_user_001",
  "whatsapp_id": "919876543210",
  "user_type": "Asha",
  "user_language": "en",
  "test_user": true
}
```

**Sample Message:**
```json
{
  "message_id": "test_msg_001",
  "user_id": "test_user_001",
  "message_text": "What is antara injection?",
  "message_type": "text",
  "timestamp": "2025-11-03T14:00:00Z"
}
```

---

### B. Common Test Utilities

**Wait for Bot Response:**
```python
def wait_for_bot_response(timestamp, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        messages = get_bot_messages_after(timestamp)
        if messages:
            return messages
        time.sleep(2)
    raise TimeoutError("No bot response received")
```

**Generate Test Message ID:**
```python
def generate_test_message_id():
    return f"wamid.test_{uuid.uuid4().hex}"
```

---

### C. Environment URLs

**Staging:**
- Base URL: `https://byoeb-staging.azurewebsites.net`
- Webhook: `https://byoeb-staging.azurewebsites.net/receive`
- Health Check: `https://byoeb-staging.azurewebsites.net/health`
- API Docs: `https://byoeb-staging.azurewebsites.net/docs`

---

### D. Contact Information

**Development Team:**
- Technical Lead: [Name, Email]
- DevOps: [Name, Email]
- QA Lead: [Name, Email]

**Escalation:**
- P0 Issues: [Phone/Slack Channel]
- Support: [Email/Ticketing System]

---

## Document Control

| Version | Date       | Author | Changes                          |
|---------|------------|--------|----------------------------------|
| 1.0     | 2025-11-03 | AI     | Initial comprehensive test plan  |

---

**End of Test Plan**

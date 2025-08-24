# 🔥 Graft Document Processing Pipeline Example

A comprehensive example demonstrating Graft's distributed workflow capabilities through a sophisticated document processing pipeline.

## 🌟 What This Example Demonstrates

This example showcases a **complex, real-world document processing workflow** that highlights Graft's key features:

### 🔧 Core Graft Features
- **Automatic Cluster Formation**: Nodes discover each other via mDNS and form clusters automatically
- **Distributed Workflow Execution**: Workflows execute across multiple nodes in the cluster  
- **State Management**: Complex document state flows through multiple processing steps
- **Error Recovery**: Failed operations trigger repair workflows and retries
- **Conditional Logic**: Workflow paths change based on document properties
- **Parallel Processing**: Multiple processing nodes execute simultaneously

### 📋 Workflow Capabilities  
- **13 Different Node Types**: Each handling specific document processing tasks
- **Quality Gates**: Documents must meet quality thresholds to proceed
- **Priority Handling**: High-priority documents get expedited processing
- **Multi-language Support**: Automatic language detection and translation
- **OCR Processing**: Images converted to text when needed
- **NLP Analysis**: Sentiment analysis and keyword extraction

## 🔄 Processing Pipeline

```
Document Ingest
       ↓
   Validator ←──── Document Repair (error recovery)
       ↓                    ↑
   [Parallel Processing]    │
   ├─ Content Analyzer      │
   └─ Content Processor     │
       ↓                    │
   Language Processor       │
       ↓                    │
   [Conditional Branches]   │
   ├─ OCR Processor (images)│
   ├─ NLP Processor         │
   └─ Quality Checker ──────┘
       ↓
   [Priority Branch]
   ├─ Priority Handler → Notification
   └─ Document Finalizer
```

## 🚀 Running the Example

1. **Navigate to the example directory**:
   ```bash
   cd examples/document-pipeline
   ```

2. **Run the example**:
   ```bash
   go run .
   ```

3. **Watch the magic happen**! The example will:
   - Start a Graft cluster
   - Register 13 different workflow nodes  
   - Process 5 different document scenarios
   - Show real-time workflow execution
   - Display comprehensive results

## 📊 Document Scenarios

The example processes 5 different scenarios:

1. **Simple Document**: Basic text processing with NLP analysis
2. **High Priority Urgent**: Priority handling with expedited processing  
3. **Image Document**: OCR processing to extract text from images
4. **Corrupted Document**: Error recovery through repair workflows
5. **Multilingual Document**: Language detection and translation

## 🔍 What You'll See

```
🚀 Starting Graft Document Processing Pipeline Example
============================================================
✅ Created Graft manager (Node: doc-processor-1, Raft: 127.0.0.1:7001, gRPC: 8001)
✅ Registered 13 workflow nodes
🔄 Starting Graft cluster...
✅ Graft cluster started successfully!

🔥 Running Document Processing Scenarios
============================================================

🔄 [1] Processing: Simple Document Processing
🔄 [2] Processing: High Priority Urgent Document
🔄 [3] Processing: Image Document with OCR
🔄 [4] Processing: Corrupted Document (Error Recovery)  
🔄 [5] Processing: Multilingual Document

⏳ Monitoring workflow executions...

[1/5] ✅ Simple Document Processing
    Workflow ID: workflow-1-1692834567
    Duration: 1.234s
    Status: completed
    Nodes Executed: 8
    📄 Final Document Status: completed
    📊 Processing Chain: [doc-processor-1-ingest, doc-processor-1-validator, ...]
```

## 🏗️ Architecture Insights

### Node Types and Responsibilities

- **DocumentIngestNode**: Entry point, classifies document type
- **DocumentValidatorNode**: Validates document integrity and content
- **ContentAnalyzerNode**: Parallel analysis of content properties  
- **ContentProcessorNode**: Core content transformation
- **LanguageProcessorNode**: Language detection and translation
- **OCRProcessorNode**: Image-to-text conversion
- **NLPProcessorNode**: Sentiment analysis and keyword extraction
- **QualityCheckerNode**: Quality scoring and gate enforcement
- **QualityEnhancerNode**: Quality improvement for failed checks
- **PriorityHandlerNode**: Expedited processing for urgent documents
- **DocumentRepairNode**: Recovery processing for corrupted documents
- **NotificationSenderNode**: Alerts for priority documents  
- **DocumentFinalizerNode**: Final processing and cleanup

### State Flow Pattern

Each node receives a `Document` struct containing:
- **Content**: The document text/data
- **Metadata**: Processing history and properties
- **Status**: Current processing state  
- **ProcessedBy**: Chain of processing nodes
- **Quality metrics**: Word count, language, priority

### Error Handling Strategy

1. **Validation Failures**: Route to repair workflows
2. **Processing Errors**: Retry with backoff
3. **Quality Failures**: Route to enhancement workflows  
4. **Network Timeouts**: Automatic retry mechanisms

## 🎯 Learning Outcomes

After running this example, you'll understand:

- How to structure complex, multi-step workflows
- Patterns for parallel and conditional workflow execution
- Error recovery and quality gate implementation
- State management in distributed workflows
- How Graft handles cluster formation automatically
- Real-world patterns for document processing pipelines

## 🔧 Customization

You can extend this example by:

- Adding new node types for specialized processing
- Implementing different quality criteria
- Adding more conditional workflow paths
- Integrating with external services (databases, APIs)
- Adding monitoring and metrics collection
- Implementing different error recovery strategies

This example serves as a comprehensive template for building production-ready distributed workflow systems with Graft.
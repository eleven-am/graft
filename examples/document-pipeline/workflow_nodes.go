package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/eleven-am/graft"
)

func marshalConfig(config ProcessingConfig) (json.RawMessage, error) {
	configBytes, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config: %w", err)
	}
	return configBytes, nil
}

type Document struct {
	ID          string                 `json:"id"`
	Content     string                 `json:"content"`
	Type        string                 `json:"type"`
	Metadata    map[string]interface{} `json:"metadata"`
	Status      string                 `json:"status"`
	WordCount   int                    `json:"word_count"`
	Language    string                 `json:"language"`
	Priority    int                    `json:"priority"`
	ProcessedBy []string               `json:"processed_by"`
}

type ProcessingConfig struct {
	MaxRetries    int           `json:"max_retries"`
	Timeout       time.Duration `json:"timeout"`
	EnableOCR     bool          `json:"enable_ocr"`
	EnableNLP     bool          `json:"enable_nlp"`
	QualityGate   float64       `json:"quality_gate"`
	ProcessorName string        `json:"processor_name"`
}

// 1. Document Ingestion Node
type DocumentIngestNode struct{}

func (n *DocumentIngestNode) GetName() string {
	return "document_ingest"
}

func (n *DocumentIngestNode) CanStart(ctx context.Context, state Document, config ProcessingConfig) bool {
	result := state.Status != "processing_complete"
	return result
}

func (n *DocumentIngestNode) Execute(ctx context.Context, doc Document, processingConfig ProcessingConfig) (*graft.NodeResult, error) {

	time.Sleep(100 * time.Millisecond)

	doc.Status = "ingested"
	doc.ProcessedBy = append(doc.ProcessedBy, processingConfig.ProcessorName+"-ingest")
	doc.Metadata = make(map[string]interface{})
	doc.Metadata["ingested_at"] = time.Now()
	doc.Metadata["source"] = "api"

	if doc.Type == "" {
		doc.Type = detectDocumentType(doc.Content)
	}

	validatorKey := fmt.Sprintf("%s-document_validator", doc.ID)
	analyzerKey := fmt.Sprintf("%s-content_analyzer", doc.ID)

	result := &graft.NodeResult{
		GlobalState: doc,
		NextNodes: []graft.NextNode{
			{NodeName: "document_validator", Config: processingConfig, IdempotencyKey: &validatorKey},
			{NodeName: "content_analyzer", Config: processingConfig, IdempotencyKey: &analyzerKey},
		},
	}

	return result, nil
}

// 2. Document Validation Node
type DocumentValidatorNode struct{}

func (n *DocumentValidatorNode) GetName() string {
	return "document_validator"
}

func (n *DocumentValidatorNode) CanStart(ctx context.Context, doc Document, config ProcessingConfig) bool {
	return true
}

func (n *DocumentValidatorNode) Execute(ctx context.Context, doc Document, processingConfig ProcessingConfig) (*graft.NodeResult, error) {
	time.Sleep(50 * time.Millisecond)

	doc.ProcessedBy = append(doc.ProcessedBy, processingConfig.ProcessorName+"-validator")

	if len(doc.Content) < 10 {
		return nil, fmt.Errorf("document content too short")
	}

	if doc.Type == "corrupted" {
		return nil, fmt.Errorf("document is corrupted and cannot be processed")
	}

	doc.Status = "validated"
	doc.Metadata["validated_at"] = time.Now()

	processorKey := fmt.Sprintf("%s-content_processor", doc.ID)

	return &graft.NodeResult{
		GlobalState: doc,
		NextNodes:   []graft.NextNode{{NodeName: "content_processor", Config: processingConfig, IdempotencyKey: &processorKey}},
	}, nil
}

// 3. Content Analyzer Node (runs in parallel)
type ContentAnalyzerNode struct{}

func (n *ContentAnalyzerNode) GetName() string {
	return "content_analyzer"
}

func (n *ContentAnalyzerNode) CanStart(ctx context.Context, doc Document, config ProcessingConfig) bool {
	return true
}

func (n *ContentAnalyzerNode) Execute(ctx context.Context, doc Document, processingConfig ProcessingConfig) (*graft.NodeResult, error) {
	time.Sleep(200 * time.Millisecond)

	doc.ProcessedBy = append(doc.ProcessedBy, processingConfig.ProcessorName+"-analyzer")
	doc.WordCount = len(strings.Fields(doc.Content))
	doc.Language = detectLanguage(doc.Content)
	doc.Priority = calculatePriority(doc)

	doc.Metadata["analyzed_at"] = time.Now()
	doc.Metadata["complexity_score"] = rand.Float64()

	langKey := fmt.Sprintf("%s-language_processor", doc.ID)
	repairKey := fmt.Sprintf("%s-document_repair", doc.ID)

	return &graft.NodeResult{
		GlobalState: doc,
		NextNodes: []graft.NextNode{
			{NodeName: "language_processor", Config: ProcessingConfig{}, IdempotencyKey: &langKey},
			{NodeName: "document_repair", Config: ProcessingConfig{}, IdempotencyKey: &repairKey},
		},
	}, nil
}

// 4. Content Processor Node
type ContentProcessorNode struct{}

func (n *ContentProcessorNode) GetName() string {
	return "content_processor"
}

func (n *ContentProcessorNode) CanStart(ctx context.Context, doc Document, config ProcessingConfig) bool {
	return doc.Status == "validated"
}

func (n *ContentProcessorNode) Execute(ctx context.Context, doc Document, processingConfig ProcessingConfig) (*graft.NodeResult, error) {
	time.Sleep(300 * time.Millisecond)

	doc.ProcessedBy = append(doc.ProcessedBy, processingConfig.ProcessorName+"-processor")

	if rand.Float64() < 0.1 {
		return nil, fmt.Errorf("processing failed due to network timeout")
	}

	doc.Content = processContent(doc.Content, doc.Type)
	doc.Status = "processed"
	doc.Metadata["processed_at"] = time.Now()

	nextNodes := []graft.NextNode{{NodeName: "quality_checker", Config: ProcessingConfig{}}}
	if processingConfig.EnableOCR && doc.Type == "image" {
		nextNodes = append(nextNodes, graft.NextNode{NodeName: "ocr_processor", Config: ProcessingConfig{}})
	}

	return &graft.NodeResult{
		GlobalState: doc,
		NextNodes:   nextNodes,
	}, nil
}

// 5. Language Processor Node (conditional based on language)
type LanguageProcessorNode struct{}

func (n *LanguageProcessorNode) GetName() string {
	return "language_processor"
}

func (n *LanguageProcessorNode) CanStart(ctx context.Context, doc Document, config ProcessingConfig) bool {
	return doc.Language != "" && doc.Language != "unknown"
}

func (n *LanguageProcessorNode) Execute(ctx context.Context, doc Document, processingConfig ProcessingConfig) (*graft.NodeResult, error) {
	time.Sleep(150 * time.Millisecond)

	doc.ProcessedBy = append(doc.ProcessedBy, processingConfig.ProcessorName+"-language")

	switch doc.Language {
	case "spanish", "french", "german":
		doc.Content = translateToEnglish(doc.Content)
		doc.Metadata["translated"] = true
	case "english":
		doc.Content = enhanceEnglishContent(doc.Content)
		doc.Metadata["enhanced"] = true
	}

	doc.Metadata["language_processed_at"] = time.Now()

	return &graft.NodeResult{
		GlobalState: doc,
		NextNodes:   []graft.NextNode{{NodeName: "nlp_processor", Config: ProcessingConfig{}}},
	}, nil
}

// 6. OCR Processor Node (for image documents)
type OCRProcessorNode struct{}

func (n *OCRProcessorNode) GetName() string {
	return "ocr_processor"
}

func (n *OCRProcessorNode) CanStart(ctx context.Context, doc Document, config ProcessingConfig) bool {
	return true
}
func (n *OCRProcessorNode) Execute(ctx context.Context, doc Document, processingConfig ProcessingConfig) (*graft.NodeResult, error) {

	time.Sleep(500 * time.Millisecond)

	doc.ProcessedBy = append(doc.ProcessedBy, processingConfig.ProcessorName+"-ocr")

	if doc.Type == "image" {
		extractedText := performOCR(doc.Content)
		doc.Content = extractedText
		doc.Type = "text"
		doc.Metadata["ocr_confidence"] = 0.95
		doc.Metadata["ocr_processed_at"] = time.Now()
	}

	return &graft.NodeResult{
		GlobalState: doc,
		NextNodes:   []graft.NextNode{{NodeName: "quality_checker", Config: ProcessingConfig{}}},
	}, nil
}

// 7. NLP Processor Node
type NLPProcessorNode struct{}

func (n *NLPProcessorNode) GetName() string {
	return "nlp_processor"
}

func (n *NLPProcessorNode) CanStart(ctx context.Context, doc Document, config ProcessingConfig) bool {
	return true
}
func (n *NLPProcessorNode) Execute(ctx context.Context, doc Document, processingConfig ProcessingConfig) (*graft.NodeResult, error) {

	time.Sleep(250 * time.Millisecond)

	doc.ProcessedBy = append(doc.ProcessedBy, processingConfig.ProcessorName+"-nlp")

	if processingConfig.EnableNLP {
		sentiment := analyzeSentiment(doc.Content)
		keywords := extractKeywords(doc.Content)

		doc.Metadata["sentiment"] = sentiment
		doc.Metadata["keywords"] = keywords
		doc.Metadata["nlp_processed_at"] = time.Now()
	}

	return &graft.NodeResult{
		GlobalState: doc,
		NextNodes:   []graft.NextNode{{NodeName: "quality_checker", Config: ProcessingConfig{}}},
	}, nil
}

// 8. Quality Checker Node (convergence point)
type QualityCheckerNode struct{}

func (n *QualityCheckerNode) GetName() string {
	return "quality_checker"
}

func (n *QualityCheckerNode) CanStart(ctx context.Context, doc Document, config ProcessingConfig) bool {
	return true
}
func (n *QualityCheckerNode) Execute(ctx context.Context, doc Document, processingConfig ProcessingConfig) (*graft.NodeResult, error) {

	time.Sleep(100 * time.Millisecond)

	doc.ProcessedBy = append(doc.ProcessedBy, processingConfig.ProcessorName+"-quality")

	qualityScore := calculateQualityScore(doc)
	doc.Metadata["quality_score"] = qualityScore
	doc.Metadata["quality_checked_at"] = time.Now()

	if qualityScore < processingConfig.QualityGate {
		return &graft.NodeResult{
			GlobalState: doc,
			NextNodes:   []graft.NextNode{{NodeName: "quality_enhancer", Config: ProcessingConfig{}}},
		}, fmt.Errorf("quality check failed: score %.2f below threshold %.2f", qualityScore, processingConfig.QualityGate)
	}

	doc.Status = "quality_approved"

	nextNodes := []graft.NextNode{{NodeName: "document_finalizer", Config: ProcessingConfig{}}}
	if doc.Priority >= 8 {
		nextNodes = append(nextNodes, graft.NextNode{NodeName: "priority_handler", Config: ProcessingConfig{}})
	}

	return &graft.NodeResult{
		GlobalState: doc,
		NextNodes:   nextNodes,
	}, nil
}

// 9. Quality Enhancer Node (error recovery)
type QualityEnhancerNode struct{}

func (n *QualityEnhancerNode) GetName() string {
	return "quality_enhancer"
}

func (n *QualityEnhancerNode) CanStart(ctx context.Context, doc Document, config ProcessingConfig) bool {
	return true
}
func (n *QualityEnhancerNode) Execute(ctx context.Context, doc Document, processingConfig ProcessingConfig) (*graft.NodeResult, error) {

	time.Sleep(200 * time.Millisecond)

	doc.ProcessedBy = append(doc.ProcessedBy, processingConfig.ProcessorName+"-enhancer")
	doc.Content = enhanceQuality(doc.Content)
	doc.Metadata["enhanced_at"] = time.Now()

	return &graft.NodeResult{
		GlobalState: doc,
		NextNodes:   []graft.NextNode{{NodeName: "quality_checker", Config: ProcessingConfig{}}},
	}, nil
}

// 10. Priority Handler Node (for high-priority documents)
type PriorityHandlerNode struct{}

func (n *PriorityHandlerNode) GetName() string {
	return "priority_handler"
}

func (n *PriorityHandlerNode) CanStart(ctx context.Context, doc Document, config ProcessingConfig) bool {
	return true
}
func (n *PriorityHandlerNode) Execute(ctx context.Context, doc Document, processingConfig ProcessingConfig) (*graft.NodeResult, error) {

	time.Sleep(50 * time.Millisecond)

	doc.ProcessedBy = append(doc.ProcessedBy, processingConfig.ProcessorName+"-priority")
	doc.Metadata["priority_processed_at"] = time.Now()
	doc.Metadata["expedited"] = true

	return &graft.NodeResult{
		GlobalState: doc,
		NextNodes:   []graft.NextNode{{NodeName: "notification_sender", Config: ProcessingConfig{}}},
	}, nil
}

// 11. Document Repair Node (error recovery)
type DocumentRepairNode struct{}

func (n *DocumentRepairNode) GetName() string {
	return "document_repair"
}

func (n *DocumentRepairNode) CanStart(ctx context.Context, doc Document, config ProcessingConfig) bool {
	return true
}
func (n *DocumentRepairNode) Execute(ctx context.Context, doc Document, processingConfig ProcessingConfig) (*graft.NodeResult, error) {

	time.Sleep(300 * time.Millisecond)

	doc.ProcessedBy = append(doc.ProcessedBy, processingConfig.ProcessorName+"-repair")

	if doc.Type == "corrupted" {
		doc.Content = repairDocument(doc.Content)
		doc.Type = "text"
		doc.Metadata["repaired"] = true
		doc.Metadata["repaired_at"] = time.Now()
	}

	validatorKey := fmt.Sprintf("%s-document_validator", doc.ID)
	return &graft.NodeResult{
		GlobalState: doc,
		NextNodes:   []graft.NextNode{{NodeName: "document_validator", Config: ProcessingConfig{}, IdempotencyKey: &validatorKey}},
	}, nil
}

// 12. Notification Sender Node
type NotificationSenderNode struct{}

func (n *NotificationSenderNode) GetName() string {
	return "notification_sender"
}

func (n *NotificationSenderNode) CanStart(ctx context.Context, doc Document, config ProcessingConfig) bool {
	return true
}
func (n *NotificationSenderNode) Execute(ctx context.Context, doc Document, processingConfig ProcessingConfig) (*graft.NodeResult, error) {

	time.Sleep(50 * time.Millisecond)

	doc.ProcessedBy = append(doc.ProcessedBy, processingConfig.ProcessorName+"-notification")

	sendNotification(doc)
	doc.Metadata["notification_sent_at"] = time.Now()

	return &graft.NodeResult{
		GlobalState: doc,
		NextNodes:   []graft.NextNode{{NodeName: "document_finalizer", Config: ProcessingConfig{}}},
	}, nil
}

// 13. Document Finalizer Node (final step)
type DocumentFinalizerNode struct{}

func (n *DocumentFinalizerNode) GetName() string {
	return "document_finalizer"
}

func (n *DocumentFinalizerNode) CanStart(ctx context.Context, doc Document, config ProcessingConfig) bool {
	return true
}
func (n *DocumentFinalizerNode) Execute(ctx context.Context, doc Document, processingConfig ProcessingConfig) (*graft.NodeResult, error) {

	time.Sleep(100 * time.Millisecond)

	doc.ProcessedBy = append(doc.ProcessedBy, processingConfig.ProcessorName+"-finalizer")
	doc.Status = "completed"
	doc.Metadata["completed_at"] = time.Now()
	doc.Metadata["total_processing_nodes"] = len(doc.ProcessedBy)

	return &graft.NodeResult{
		GlobalState: doc,
		NextNodes:   []graft.NextNode{},
	}, nil
}

// Helper functions (simulated processing)

func detectDocumentType(content string) string {
	if strings.Contains(content, "ERROR") {
		return "corrupted"
	}
	if strings.Contains(content, "IMAGE_DATA") {
		return "image"
	}
	if len(content) > 1000 {
		return "document"
	}
	return "text"
}

func detectLanguage(content string) string {
	content = strings.ToLower(content)
	if strings.Contains(content, "hola") || strings.Contains(content, "gracias") {
		return "spanish"
	}
	if strings.Contains(content, "bonjour") || strings.Contains(content, "merci") {
		return "french"
	}
	if strings.Contains(content, "guten") || strings.Contains(content, "danke") {
		return "german"
	}
	if strings.Contains(content, "hello") || strings.Contains(content, "thank") {
		return "english"
	}
	return "unknown"
}

func calculatePriority(doc Document) int {
	priority := 5
	if strings.Contains(strings.ToLower(doc.Content), "urgent") {
		priority += 3
	}
	if doc.WordCount > 1000 {
		priority += 2
	}
	if doc.Type == "image" {
		priority += 1
	}
	if priority > 10 {
		priority = 10
	}
	return priority
}

func processContent(content, docType string) string {
	switch docType {
	case "document":
		return fmt.Sprintf("[PROCESSED_DOC] %s", content)
	case "image":
		return fmt.Sprintf("[PROCESSED_IMG] %s", content)
	default:
		return fmt.Sprintf("[PROCESSED] %s", content)
	}
}

func translateToEnglish(content string) string {
	return fmt.Sprintf("[TRANSLATED] %s", content)
}

func enhanceEnglishContent(content string) string {
	return fmt.Sprintf("[ENHANCED] %s", content)
}

func performOCR(content string) string {
	return strings.ReplaceAll(content, "IMAGE_DATA", "EXTRACTED_TEXT")
}

func analyzeSentiment(content string) string {
	if strings.Contains(strings.ToLower(content), "good") ||
		strings.Contains(strings.ToLower(content), "excellent") {
		return "positive"
	}
	if strings.Contains(strings.ToLower(content), "bad") ||
		strings.Contains(strings.ToLower(content), "terrible") {
		return "negative"
	}
	return "neutral"
}

func extractKeywords(content string) []string {
	words := strings.Fields(strings.ToLower(content))
	keywords := []string{}
	for _, word := range words {
		if len(word) > 5 && !strings.Contains(word, "the") {
			keywords = append(keywords, word)
		}
		if len(keywords) >= 5 {
			break
		}
	}
	return keywords
}

func calculateQualityScore(doc Document) float64 {
	score := 0.7
	if doc.WordCount > 100 {
		score += 0.1
	}
	if doc.Language != "unknown" {
		score += 0.1
	}
	if len(doc.ProcessedBy) > 5 {
		score += 0.1
	}

	score += rand.Float64() * 0.1

	if score > 1.0 {
		score = 1.0
	}
	return score
}

func enhanceQuality(content string) string {
	return fmt.Sprintf("[ENHANCED_QUALITY] %s", content)
}

func repairDocument(content string) string {
	return strings.ReplaceAll(content, "ERROR", "REPAIRED")
}

func sendNotification(doc Document) {
	fmt.Printf("PRIORITY NOTIFICATION: Document %s completed with %d processing steps\n",
		doc.ID, len(doc.ProcessedBy))
}

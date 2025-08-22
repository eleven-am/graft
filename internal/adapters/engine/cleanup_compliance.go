package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type ComplianceConfig struct {
	Mode                     string                   `json:"mode"`
	RequireAuthorization     bool                     `json:"require_authorization"`
	RetentionRequirements    map[string]time.Duration `json:"retention_requirements"`
	AuditTrailRetention      time.Duration            `json:"audit_trail_retention"`
	ComplianceFrameworks     []string                 `json:"compliance_frameworks"`
	DataClassifications      []string                 `json:"data_classifications"`
	ApprovalWorkflowRequired bool                     `json:"approval_workflow_required"`
}

type ComplianceManager struct {
	config     ComplianceConfig
	storage    ports.StoragePort
	logger     *slog.Logger
	authorizer AuthorizationService
	auditTrail AuditTrailService
}

type AuthorizationService interface {
	AuthorizeCleanup(ctx context.Context, workflowID string, requester string, operations []Operation) error
	RequiresApproval(workflowID string, operations []Operation) bool
	GetApprovalStatus(ctx context.Context, requestID string) (ApprovalStatus, error)
}

type AuditTrailService interface {
	RecordCleanupEvent(ctx context.Context, event AuditEvent) error
	GetCleanupHistory(ctx context.Context, workflowID string) ([]AuditEvent, error)
	CreateCleanupReport(ctx context.Context, criteria ReportCriteria) (CleanupReport, error)
}

type Operation struct {
	Target string `json:"target"`
	Action string `json:"action"`
}

type ApprovalStatus struct {
	Status     string    `json:"status"`
	ApprovedBy string    `json:"approved_by,omitempty"`
	ApprovedAt time.Time `json:"approved_at,omitempty"`
	Reason     string    `json:"reason,omitempty"`
}

type AuditEvent struct {
	EventID        string                 `json:"event_id"`
	WorkflowID     string                 `json:"workflow_id"`
	EventType      string                 `json:"event_type"`
	Timestamp      time.Time              `json:"timestamp"`
	Requester      string                 `json:"requester"`
	Operations     []Operation            `json:"operations"`
	Status         string                 `json:"status"`
	Details        map[string]interface{} `json:"details,omitempty"`
	ComplianceInfo ComplianceInfo         `json:"compliance_info"`
}

type ComplianceInfo struct {
	Framework        string        `json:"framework"`
	DataClass        string        `json:"data_class"`
	RetentionPeriod  time.Duration `json:"retention_period"`
	AuthorizationRef string        `json:"authorization_ref,omitempty"`
}

type ReportCriteria struct {
	StartDate   time.Time `json:"start_date"`
	EndDate     time.Time `json:"end_date"`
	WorkflowIDs []string  `json:"workflow_ids,omitempty"`
	Framework   string    `json:"framework,omitempty"`
}

type CleanupReport struct {
	GeneratedAt        time.Time      `json:"generated_at"`
	Criteria           ReportCriteria `json:"criteria"`
	TotalEvents        int            `json:"total_events"`
	SuccessfulCleanups int            `json:"successful_cleanups"`
	FailedCleanups     int            `json:"failed_cleanups"`
	ComplianceScore    float64        `json:"compliance_score"`
	Events             []AuditEvent   `json:"events"`
	Summary            ReportSummary  `json:"summary"`
}

type ReportSummary struct {
	WorkflowsProcessed  int                   `json:"workflows_processed"`
	DataTypesProcessed  map[string]int        `json:"data_types_processed"`
	RetentionCompliance map[string]bool       `json:"retention_compliance"`
	ViolationsFound     []ComplianceViolation `json:"violations_found"`
}

type ComplianceViolation struct {
	WorkflowID    string    `json:"workflow_id"`
	ViolationType string    `json:"violation_type"`
	Description   string    `json:"description"`
	Severity      string    `json:"severity"`
	DetectedAt    time.Time `json:"detected_at"`
}

func NewComplianceManager(config ComplianceConfig, storage ports.StoragePort, logger *slog.Logger, authorizer AuthorizationService, auditTrail AuditTrailService) *ComplianceManager {
	return &ComplianceManager{
		config:     config,
		storage:    storage,
		logger:     logger.With("component", "compliance_manager"),
		authorizer: authorizer,
		auditTrail: auditTrail,
	}
}

func (cm *ComplianceManager) ValidateCleanupRequest(ctx context.Context, workflowID string, requester string, operations []Operation) error {
	cm.logger.Info("validating cleanup request for compliance",
		"workflow_id", workflowID,
		"requester", requester,
		"mode", cm.config.Mode)

	if cm.config.Mode == "audit_only" {
		cm.logger.Info("compliance mode is audit_only, allowing cleanup to proceed",
			"workflow_id", workflowID)
		return nil
	}

	if err := cm.validateRetentionRequirements(ctx, workflowID); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "retention requirements validation failed",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	if cm.config.RequireAuthorization {
		if err := cm.authorizer.AuthorizeCleanup(ctx, workflowID, requester, operations); err != nil {
			return domain.Error{
				Type:    domain.ErrorTypeUnauthorized,
				Message: "cleanup authorization failed",
				Details: map[string]interface{}{
					"workflow_id": workflowID,
					"requester":   requester,
					"error":       err.Error(),
				},
			}
		}
	}

	if cm.config.ApprovalWorkflowRequired {
		if cm.authorizer.RequiresApproval(workflowID, operations) {
			return domain.Error{
				Type:    domain.ErrorTypeConflict,
				Message: "cleanup requires approval workflow",
				Details: map[string]interface{}{
					"workflow_id": workflowID,
					"requester":   requester,
					"reason":      "request must be submitted for approval",
				},
			}
		}
	}

	return nil
}

func (cm *ComplianceManager) RecordCleanupAttempt(ctx context.Context, workflowID string, requester string, operations []Operation, status string, details map[string]interface{}) error {
	event := AuditEvent{
		EventID:        generateEventID(),
		WorkflowID:     workflowID,
		EventType:      "cleanup_attempt",
		Timestamp:      time.Now(),
		Requester:      requester,
		Operations:     operations,
		Status:         status,
		Details:        details,
		ComplianceInfo: cm.buildComplianceInfo(workflowID),
	}

	if err := cm.auditTrail.RecordCleanupEvent(ctx, event); err != nil {
		cm.logger.Error("failed to record cleanup audit event",
			"workflow_id", workflowID,
			"error", err)
		return err
	}

	cm.logger.Info("cleanup attempt recorded in audit trail",
		"workflow_id", workflowID,
		"event_id", event.EventID,
		"status", status)

	return nil
}

func (cm *ComplianceManager) GenerateComplianceReport(ctx context.Context, criteria ReportCriteria) (CleanupReport, error) {
	cm.logger.Info("generating compliance report",
		"start_date", criteria.StartDate,
		"end_date", criteria.EndDate,
		"framework", criteria.Framework)

	report, err := cm.auditTrail.CreateCleanupReport(ctx, criteria)
	if err != nil {
		return CleanupReport{}, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create cleanup report",
			Details: map[string]interface{}{
				"criteria": criteria,
				"error":    err.Error(),
			},
		}
	}

	report.ComplianceScore = cm.calculateComplianceScore(report)
	report.Summary.ViolationsFound = cm.detectViolations(report.Events)

	cm.logger.Info("compliance report generated",
		"total_events", report.TotalEvents,
		"compliance_score", report.ComplianceScore,
		"violations", len(report.Summary.ViolationsFound))

	return report, nil
}

func (cm *ComplianceManager) GetCleanupHistory(ctx context.Context, workflowID string) ([]AuditEvent, error) {
	return cm.auditTrail.GetCleanupHistory(ctx, workflowID)
}

func (cm *ComplianceManager) validateRetentionRequirements(ctx context.Context, workflowID string) error {
	stateKey := fmt.Sprintf("workflow:state:%s", workflowID)
	data, err := cm.storage.Get(ctx, stateKey)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to get workflow state",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	var workflowData map[string]interface{}
	if err := json.Unmarshal(data, &workflowData); err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to unmarshal workflow state",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
				"error":       err.Error(),
			},
		}
	}

	completedAtStr, ok := workflowData["completed_at"].(string)
	if !ok || completedAtStr == "" {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "workflow completion time not found or invalid",
			Details: map[string]interface{}{
				"workflow_id": workflowID,
			},
		}
	}

	completedAt, err := time.Parse(time.RFC3339, completedAtStr)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "failed to parse workflow completion time",
			Details: map[string]interface{}{
				"workflow_id":     workflowID,
				"completion_time": completedAtStr,
				"error":           err.Error(),
			},
		}
	}

	dataClass := cm.determineDataClassification(workflowData)
	requiredRetention, exists := cm.config.RetentionRequirements[dataClass]
	if !exists {
		requiredRetention = cm.config.RetentionRequirements["default"]
	}

	if time.Since(completedAt) < requiredRetention {
		return domain.Error{
			Type:    domain.ErrorTypeConflict,
			Message: "workflow has not met minimum retention requirement",
			Details: map[string]interface{}{
				"workflow_id":           workflowID,
				"required_retention":    requiredRetention.String(),
				"time_since_completion": time.Since(completedAt).String(),
				"completed_at":          completedAt.Format(time.RFC3339),
				"data_class":            dataClass,
			},
		}
	}

	return nil
}

func (cm *ComplianceManager) buildComplianceInfo(workflowID string) ComplianceInfo {
	framework := "default"
	if len(cm.config.ComplianceFrameworks) > 0 {
		framework = cm.config.ComplianceFrameworks[0]
	}

	dataClass := "standard"
	if len(cm.config.DataClassifications) > 0 {
		dataClass = cm.config.DataClassifications[0]
	}

	retentionPeriod := cm.config.RetentionRequirements[dataClass]
	if retentionPeriod == 0 {
		retentionPeriod = cm.config.RetentionRequirements["default"]
	}

	return ComplianceInfo{
		Framework:       framework,
		DataClass:       dataClass,
		RetentionPeriod: retentionPeriod,
	}
}

func (cm *ComplianceManager) determineDataClassification(workflowData map[string]interface{}) string {
	metadata, ok := workflowData["metadata"].(map[string]interface{})
	if !ok {
		return "standard"
	}

	if classification, exists := metadata["data_classification"].(string); exists {
		return classification
	}

	if sensitive, exists := metadata["sensitive"].(bool); exists && sensitive {
		return "sensitive"
	}

	return "standard"
}

func (cm *ComplianceManager) calculateComplianceScore(report CleanupReport) float64 {
	if report.TotalEvents == 0 {
		return 100.0
	}

	violations := float64(len(report.Summary.ViolationsFound))
	total := float64(report.TotalEvents)

	score := ((total - violations) / total) * 100.0
	if score < 0 {
		score = 0
	}

	return score
}

func (cm *ComplianceManager) detectViolations(events []AuditEvent) []ComplianceViolation {
	var violations []ComplianceViolation

	for _, event := range events {
		if event.Status == "failed" {
			violations = append(violations, ComplianceViolation{
				WorkflowID:    event.WorkflowID,
				ViolationType: "cleanup_failure",
				Description:   "Cleanup operation failed to complete",
				Severity:      "medium",
				DetectedAt:    event.Timestamp,
			})
		}

		if cm.isRetentionViolation(event) {
			violations = append(violations, ComplianceViolation{
				WorkflowID:    event.WorkflowID,
				ViolationType: "retention_violation",
				Description:   "Data cleaned before minimum retention period",
				Severity:      "high",
				DetectedAt:    event.Timestamp,
			})
		}
	}

	return violations
}

func (cm *ComplianceManager) isRetentionViolation(event AuditEvent) bool {
	return false
}

func generateEventID() string {
	return fmt.Sprintf("audit_%d", time.Now().UnixNano())
}

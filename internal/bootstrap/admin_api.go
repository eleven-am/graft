package bootstrap

import (
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
)

type AdminAPIConfig struct {
	SecretsManager *SecretsManager
	Bootstrapper   *Bootstrapper
	MetaStore      MetaStore
	FencingManager *FencingManager
	TokenGenerator *ForceBootstrapTokenGenerator
	Executor       *ForceBootstrapExecutor
	Logger         *slog.Logger
}

type AdminAPI struct {
	secretsManager *SecretsManager
	bootstrapper   *Bootstrapper
	metaStore      MetaStore
	fencingManager *FencingManager
	tokenGenerator *ForceBootstrapTokenGenerator
	executor       *ForceBootstrapExecutor
	logger         *slog.Logger
}

func NewAdminAPI(config AdminAPIConfig) *AdminAPI {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &AdminAPI{
		secretsManager: config.SecretsManager,
		bootstrapper:   config.Bootstrapper,
		metaStore:      config.MetaStore,
		fencingManager: config.FencingManager,
		tokenGenerator: config.TokenGenerator,
		executor:       config.Executor,
		logger:         logger.With("component", "admin_api"),
	}
}

func (a *AdminAPI) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/admin/bootstrap/status", a.withAuth(a.HandleStatus))
	mux.HandleFunc("/admin/bootstrap/force", a.withAuth(a.HandleForce))
}

type StatusResponse struct {
	State        string            `json:"state"`
	ClusterUUID  string            `json:"cluster_uuid,omitempty"`
	FencingEpoch uint64            `json:"fencing_epoch"`
	IsReady      bool              `json:"is_ready"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

func (a *AdminAPI) HandleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		a.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	response := StatusResponse{
		State:    string(StateUninitialized),
		IsReady:  false,
		Metadata: make(map[string]string),
	}

	if a.bootstrapper != nil {
		response.State = string(a.bootstrapper.CurrentState())
		response.IsReady = a.bootstrapper.IsReady()
	}

	if a.metaStore != nil {
		meta, err := a.metaStore.LoadMeta()
		if err == nil && meta != nil {
			response.ClusterUUID = meta.ClusterUUID
			response.FencingEpoch = meta.FencingEpoch
		}
	}

	if a.fencingManager != nil {
		response.FencingEpoch = a.fencingManager.CurrentEpoch()
	}

	a.writeJSON(w, http.StatusOK, response)
}

type ForceBootstrapRequest struct {
	Token string `json:"token"`
}

type ForceBootstrapResponse struct {
	Success     bool   `json:"success"`
	Message     string `json:"message"`
	NewEpoch    uint64 `json:"new_epoch,omitempty"`
	ClusterUUID string `json:"cluster_uuid,omitempty"`
}

func (a *AdminAPI) HandleForce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		a.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req ForceBootstrapRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		a.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Token == "" {
		a.writeError(w, http.StatusBadRequest, "token is required")
		return
	}

	if a.tokenGenerator == nil {
		a.writeError(w, http.StatusServiceUnavailable, "token generator not configured")
		return
	}

	if a.executor == nil {
		a.writeError(w, http.StatusServiceUnavailable, "force bootstrap executor not configured")
		return
	}

	currentState := StateUninitialized
	if a.bootstrapper != nil {
		currentState = a.bootstrapper.CurrentState()
	}

	if !a.isForceBootstrapAllowed(currentState) {
		a.logger.Warn("force bootstrap rejected due to invalid state",
			slog.String("current_state", string(currentState)))
		a.writeError(w, http.StatusConflict,
			"force bootstrap not allowed in current state: "+string(currentState))
		return
	}

	token, err := a.tokenGenerator.Validate(r.Context(), req.Token)
	if err != nil {
		a.logger.Warn("force bootstrap token validation failed",
			slog.Any("error", err))
		a.writeError(w, http.StatusForbidden, "invalid or expired token")
		return
	}

	a.logger.Info("executing force bootstrap",
		slog.String("cluster_uuid", token.ClusterUUID),
		slog.String("reason", token.Reason),
		slog.String("issued_by", token.IssuedBy),
		slog.String("current_state", string(currentState)))

	if err := a.executor.Execute(r.Context(), token); err != nil {
		a.logger.Error("force bootstrap execution failed",
			slog.Any("error", err))

		if blocked, ok := err.(*ForceBootstrapBlockedError); ok {
			a.writeJSON(w, http.StatusConflict, ForceBootstrapResponse{
				Success: false,
				Message: blocked.Reason,
			})
			return
		}

		a.writeError(w, http.StatusInternalServerError, "force bootstrap failed: "+err.Error())
		return
	}

	var newEpoch uint64
	if a.fencingManager != nil {
		newEpoch = a.fencingManager.CurrentEpoch()
	}

	a.logger.Info("force bootstrap completed successfully",
		slog.String("cluster_uuid", token.ClusterUUID),
		slog.Uint64("new_epoch", newEpoch))

	response := ForceBootstrapResponse{
		Success:     true,
		Message:     "force bootstrap completed successfully",
		NewEpoch:    newEpoch,
		ClusterUUID: token.ClusterUUID,
	}

	a.writeJSON(w, http.StatusOK, response)
}

func (a *AdminAPI) isForceBootstrapAllowed(state NodeState) bool {
	switch state {
	case StateFenced, StateDegraded, StateAwaitingWipe, StateUninitialized:
		return true
	default:
		return false
	}
}

func (a *AdminAPI) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.secretsManager == nil || !a.secretsManager.HasAdminToken() {
			a.writeError(w, http.StatusServiceUnavailable, "admin token not configured")
			return
		}

		auth := r.Header.Get("Authorization")
		if auth == "" {
			a.writeError(w, http.StatusUnauthorized, "missing authorization header")
			return
		}

		const prefix = "Bearer "
		if !strings.HasPrefix(auth, prefix) {
			a.writeError(w, http.StatusUnauthorized, "invalid authorization format")
			return
		}

		token := auth[len(prefix):]
		expected := a.secretsManager.AdminToken()

		if subtle.ConstantTimeCompare([]byte(token), expected) != 1 {
			a.writeError(w, http.StatusUnauthorized, "invalid token")
			return
		}

		next(w, r)
	}
}

func (a *AdminAPI) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		a.logger.Error("failed to encode response",
			slog.Any("error", err))
	}
}

type errorResponse struct {
	Error string `json:"error"`
}

func (a *AdminAPI) writeError(w http.ResponseWriter, status int, message string) {
	a.writeJSON(w, status, errorResponse{Error: message})
}

package bootstrap

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAdminAPI_RegisterRoutes(t *testing.T) {
	api := NewAdminAPI(AdminAPIConfig{})
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/admin/bootstrap/status")
	if err != nil {
		t.Fatalf("failed to make request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 (no admin token), got %d", resp.StatusCode)
	}
}

func TestAdminAPI_HandleStatus_Success(t *testing.T) {
	secrets := NewSecretsManager(SecretsConfig{}, nil)
	secrets.SetAdminToken([]byte("test-admin-token"))

	api := NewAdminAPI(AdminAPIConfig{
		SecretsManager: secrets,
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/bootstrap/status", nil)
	req.Header.Set("Authorization", "Bearer test-admin-token")
	w := httptest.NewRecorder()

	api.withAuth(api.HandleStatus)(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var resp StatusResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.State != string(StateUninitialized) {
		t.Errorf("expected state %q, got %q", StateUninitialized, resp.State)
	}
}

func TestAdminAPI_HandleStatus_NoAuth(t *testing.T) {
	secrets := NewSecretsManager(SecretsConfig{}, nil)
	secrets.SetAdminToken([]byte("test-admin-token"))

	api := NewAdminAPI(AdminAPIConfig{
		SecretsManager: secrets,
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/bootstrap/status", nil)
	w := httptest.NewRecorder()

	api.withAuth(api.HandleStatus)(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401, got %d", w.Code)
	}
}

func TestAdminAPI_HandleStatus_InvalidAuth(t *testing.T) {
	secrets := NewSecretsManager(SecretsConfig{}, nil)
	secrets.SetAdminToken([]byte("correct-token"))

	api := NewAdminAPI(AdminAPIConfig{
		SecretsManager: secrets,
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/bootstrap/status", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	w := httptest.NewRecorder()

	api.withAuth(api.HandleStatus)(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401, got %d", w.Code)
	}
}

func TestAdminAPI_HandleStatus_InvalidFormat(t *testing.T) {
	secrets := NewSecretsManager(SecretsConfig{}, nil)
	secrets.SetAdminToken([]byte("test-token"))

	api := NewAdminAPI(AdminAPIConfig{
		SecretsManager: secrets,
	})

	tests := []struct {
		name   string
		header string
	}{
		{"basic auth", "Basic dXNlcjpwYXNz"},
		{"no prefix", "test-token"},
		{"lowercase bearer", "bearer test-token"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/admin/bootstrap/status", nil)
			req.Header.Set("Authorization", tt.header)
			w := httptest.NewRecorder()

			api.withAuth(api.HandleStatus)(w, req)

			if w.Code != http.StatusUnauthorized {
				t.Errorf("expected status 401, got %d", w.Code)
			}
		})
	}
}

func TestAdminAPI_HandleStatus_MethodNotAllowed(t *testing.T) {
	secrets := NewSecretsManager(SecretsConfig{}, nil)
	secrets.SetAdminToken([]byte("test-admin-token"))

	api := NewAdminAPI(AdminAPIConfig{
		SecretsManager: secrets,
	})

	req := httptest.NewRequest(http.MethodPost, "/admin/bootstrap/status", nil)
	req.Header.Set("Authorization", "Bearer test-admin-token")
	w := httptest.NewRecorder()

	api.withAuth(api.HandleStatus)(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", w.Code)
	}
}

func TestAdminAPI_HandleForce_NoAuth(t *testing.T) {
	secrets := NewSecretsManager(SecretsConfig{}, nil)
	secrets.SetAdminToken([]byte("test-admin-token"))

	api := NewAdminAPI(AdminAPIConfig{
		SecretsManager: secrets,
	})

	body := bytes.NewBufferString(`{"token":"some-force-token"}`)
	req := httptest.NewRequest(http.MethodPost, "/admin/bootstrap/force", body)
	w := httptest.NewRecorder()

	api.withAuth(api.HandleForce)(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401, got %d", w.Code)
	}
}

func TestAdminAPI_HandleForce_MethodNotAllowed(t *testing.T) {
	secrets := NewSecretsManager(SecretsConfig{}, nil)
	secrets.SetAdminToken([]byte("test-admin-token"))

	api := NewAdminAPI(AdminAPIConfig{
		SecretsManager: secrets,
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/bootstrap/force", nil)
	req.Header.Set("Authorization", "Bearer test-admin-token")
	w := httptest.NewRecorder()

	api.withAuth(api.HandleForce)(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", w.Code)
	}
}

func TestAdminAPI_HandleForce_InvalidBody(t *testing.T) {
	secrets := NewSecretsManager(SecretsConfig{}, nil)
	secrets.SetAdminToken([]byte("test-admin-token"))

	api := NewAdminAPI(AdminAPIConfig{
		SecretsManager: secrets,
	})

	body := bytes.NewBufferString(`{invalid json}`)
	req := httptest.NewRequest(http.MethodPost, "/admin/bootstrap/force", body)
	req.Header.Set("Authorization", "Bearer test-admin-token")
	w := httptest.NewRecorder()

	api.withAuth(api.HandleForce)(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestAdminAPI_HandleForce_EmptyToken(t *testing.T) {
	secrets := NewSecretsManager(SecretsConfig{}, nil)
	secrets.SetAdminToken([]byte("test-admin-token"))

	api := NewAdminAPI(AdminAPIConfig{
		SecretsManager: secrets,
	})

	body := bytes.NewBufferString(`{"token":""}`)
	req := httptest.NewRequest(http.MethodPost, "/admin/bootstrap/force", body)
	req.Header.Set("Authorization", "Bearer test-admin-token")
	w := httptest.NewRecorder()

	api.withAuth(api.HandleForce)(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestAdminAPI_HandleForce_NoTokenGenerator(t *testing.T) {
	secrets := NewSecretsManager(SecretsConfig{}, nil)
	secrets.SetAdminToken([]byte("test-admin-token"))

	api := NewAdminAPI(AdminAPIConfig{
		SecretsManager: secrets,
	})

	body := bytes.NewBufferString(`{"token":"some-token"}`)
	req := httptest.NewRequest(http.MethodPost, "/admin/bootstrap/force", body)
	req.Header.Set("Authorization", "Bearer test-admin-token")
	w := httptest.NewRecorder()

	api.withAuth(api.HandleForce)(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", w.Code)
	}
}

func TestAdminAPI_WithAuth_NoSecretsManager(t *testing.T) {
	api := NewAdminAPI(AdminAPIConfig{})

	req := httptest.NewRequest(http.MethodGet, "/admin/bootstrap/status", nil)
	req.Header.Set("Authorization", "Bearer some-token")
	w := httptest.NewRecorder()

	api.withAuth(api.HandleStatus)(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", w.Code)
	}
}

func TestAdminAPI_WriteJSON(t *testing.T) {
	api := NewAdminAPI(AdminAPIConfig{})

	w := httptest.NewRecorder()
	data := map[string]string{"key": "value"}

	api.writeJSON(w, http.StatusOK, data)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected content-type application/json, got %s", contentType)
	}

	var result map[string]string
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result["key"] != "value" {
		t.Errorf("expected value 'value', got %s", result["key"])
	}
}

func TestAdminAPI_WriteError(t *testing.T) {
	api := NewAdminAPI(AdminAPIConfig{})

	w := httptest.NewRecorder()

	api.writeError(w, http.StatusBadRequest, "test error")

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}

	var result errorResponse
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Error != "test error" {
		t.Errorf("expected error 'test error', got %s", result.Error)
	}
}

func TestAdminAPI_HandleStatus_WithBootstrapper(t *testing.T) {
	secrets := NewSecretsManager(SecretsConfig{}, nil)
	secrets.SetAdminToken([]byte("test-admin-token"))

	metaStore := newMockMetaStore()
	metaStore.meta = &ClusterMeta{
		ClusterUUID:  "test-uuid-123",
		FencingEpoch: 5,
		State:        StateReady,
	}

	api := NewAdminAPI(AdminAPIConfig{
		SecretsManager: secrets,
		MetaStore:      metaStore,
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/bootstrap/status", nil)
	req.Header.Set("Authorization", "Bearer test-admin-token")
	w := httptest.NewRecorder()

	api.withAuth(api.HandleStatus)(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var resp StatusResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.ClusterUUID != "test-uuid-123" {
		t.Errorf("expected cluster UUID 'test-uuid-123', got %s", resp.ClusterUUID)
	}

	if resp.FencingEpoch != 5 {
		t.Errorf("expected fencing epoch 5, got %d", resp.FencingEpoch)
	}
}

func TestAdminAPI_HandleForce_NoExecutor(t *testing.T) {
	secrets := NewSecretsManager(SecretsConfig{}, nil)
	secrets.SetAdminToken([]byte("test-admin-token"))
	secrets.SetForceBootstrapKey([]byte("test-force-key-32-bytes-long!!!"))

	tokenGenerator := NewForceBootstrapTokenGenerator("", nil, nil, secrets, nil)

	api := NewAdminAPI(AdminAPIConfig{
		SecretsManager: secrets,
		TokenGenerator: tokenGenerator,
	})

	body := bytes.NewBufferString(`{"token":"some-token"}`)
	req := httptest.NewRequest(http.MethodPost, "/admin/bootstrap/force", body)
	req.Header.Set("Authorization", "Bearer test-admin-token")
	w := httptest.NewRecorder()

	api.withAuth(api.HandleForce)(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", w.Code)
	}
}

type mockBootstrapperForState struct {
	state NodeState
}

func (m *mockBootstrapperForState) CurrentState() NodeState {
	return m.state
}

func (m *mockBootstrapperForState) IsReady() bool {
	return m.state == StateReady
}

func TestAdminAPI_HandleForce_StateCheckReady(t *testing.T) {
	secrets := NewSecretsManager(SecretsConfig{}, nil)
	secrets.SetAdminToken([]byte("test-admin-token"))
	secrets.SetForceBootstrapKey([]byte("test-force-key-32-bytes-long!!!"))

	tokenGenerator := NewForceBootstrapTokenGenerator("", nil, nil, secrets, nil)

	metaStore := newMockMetaStore()
	metaStore.meta = &ClusterMeta{
		State: StateReady,
	}
	metaStore.exists = true

	sm, _ := NewStateMachine(StateMachineDeps{MetaStore: metaStore})
	_ = sm.Initialize()

	bootstrapper := &Bootstrapper{
		stateMachine: sm,
	}

	api := NewAdminAPI(AdminAPIConfig{
		SecretsManager: secrets,
		TokenGenerator: tokenGenerator,
		Bootstrapper:   bootstrapper,
		Executor:       &ForceBootstrapExecutor{},
	})

	body := bytes.NewBufferString(`{"token":"some-token"}`)
	req := httptest.NewRequest(http.MethodPost, "/admin/bootstrap/force", body)
	req.Header.Set("Authorization", "Bearer test-admin-token")
	w := httptest.NewRecorder()

	api.withAuth(api.HandleForce)(w, req)

	if w.Code != http.StatusConflict {
		t.Errorf("expected status 409 (conflict), got %d", w.Code)
	}
}

func TestAdminAPI_isForceBootstrapAllowed(t *testing.T) {
	api := NewAdminAPI(AdminAPIConfig{})

	tests := []struct {
		state   NodeState
		allowed bool
	}{
		{StateFenced, true},
		{StateDegraded, true},
		{StateAwaitingWipe, true},
		{StateUninitialized, true},
		{StateReady, false},
		{StateBootstrapping, false},
		{StateRecovering, false},
		{StateJoining, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			result := api.isForceBootstrapAllowed(tt.state)
			if result != tt.allowed {
				t.Errorf("state %s: expected allowed=%t, got %t", tt.state, tt.allowed, result)
			}
		})
	}
}

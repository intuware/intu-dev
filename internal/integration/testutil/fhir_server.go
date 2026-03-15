//go:build integration

package testutil

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"
)

// FHIRTestServer is a minimal in-process FHIR R4 server for integration tests.
// It serves metadata, Patient create/search, and Subscription create with rest-hook delivery.
type FHIRTestServer struct {
	server *httptest.Server
	mu     sync.Mutex
	// patients keyed by id; value has meta.lastUpdated for search
	patients    []map[string]any
	subscriptions []fhirSubscription
}

type fhirSubscription struct {
	ID       string
	Criteria string
	Endpoint string
}

// NewFHIRTestServer starts an in-process FHIR server and returns its base URL (e.g. http://127.0.0.1:port/fhir).
func NewFHIRTestServer() *FHIRTestServer {
	f := &FHIRTestServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("/fhir/metadata", f.serveMetadata)
	mux.HandleFunc("/fhir/Patient", f.servePatient)
	mux.HandleFunc("/fhir/Subscription", f.serveSubscription)
	f.server = httptest.NewServer(mux)
	return f
}

// BaseURL returns the FHIR API base URL (e.g. http://127.0.0.1:port/fhir).
func (f *FHIRTestServer) BaseURL() string {
	return strings.TrimSuffix(f.server.URL, "/") + "/fhir"
}

// Close shuts down the server.
func (f *FHIRTestServer) Close() {
	if f.server != nil {
		f.server.Close()
	}
}

func (f *FHIRTestServer) serveMetadata(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/fhir+json")
	json.NewEncoder(w).Encode(map[string]any{
		"resourceType": "CapabilityStatement",
		"status":       "active",
		"fhirVersion":  "4.0.1",
		"rest": []map[string]any{
			{
				"mode": "server",
				"resource": []map[string]any{
					{"type": "Patient", "interaction": []map[string]any{{"code": "read"}, {"code": "create"}, {"code": "search-type"}}},
					{"type": "Subscription", "interaction": []map[string]any{{"code": "create"}}},
				},
			},
		},
	})
}

func (f *FHIRTestServer) servePatient(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/fhir+json")

	switch r.Method {
	case http.MethodPost:
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		if body["resourceType"] != "Patient" {
			http.Error(w, "Not a Patient", http.StatusBadRequest)
			return
		}
		if body["id"] == nil || body["id"] == "" {
			body["id"] = "gen-" + time.Now().Format("20060102150405.000")
		}
		body["meta"] = map[string]any{"lastUpdated": time.Now().UTC().Format("2006-01-02T15:04:05Z07:00")}
		f.mu.Lock()
		f.patients = append(f.patients, body)
		subs := make([]fhirSubscription, len(f.subscriptions))
		copy(subs, f.subscriptions)
		f.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{"resourceType": "OperationOutcome", "issue": []map[string]any{{"severity": "information", "code": "informational", "diagnostics": "Created"}}})
		// Deliver rest-hook notifications asynchronously
		go f.deliverSubscriptions(subs, body)
		return

	case http.MethodGet:
		f.mu.Lock()
		patients := make([]map[string]any, len(f.patients))
		copy(patients, f.patients)
		f.mu.Unlock()
		// FHIR _lastUpdated: value is often "ge<timestamp>" (greater-or-equal); include resource if lastUpdated >= that timestamp
		sinceParam := r.URL.Query().Get("_lastUpdated")
		since := strings.TrimPrefix(sinceParam, "ge")
		var entries []map[string]any
		for _, p := range patients {
			if since != "" {
				meta, _ := p["meta"].(map[string]any)
				lu, _ := meta["lastUpdated"].(string)
				if lu < since {
					continue
				}
			}
			entries = append(entries, map[string]any{"resource": p})
		}
		json.NewEncoder(w).Encode(map[string]any{
			"resourceType": "Bundle",
			"type":         "searchset",
			"total":        len(entries),
			"entry":        entries,
		})
		return
	}

	http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}

func (f *FHIRTestServer) deliverSubscriptions(subs []fhirSubscription, patient map[string]any) {
	// Simple criteria match: "Patient" or "Patient?name=X" (match name in patient if present)
	for _, sub := range subs {
		if !f.criteriaMatches(sub.Criteria, patient) {
			continue
		}
		// POST notification to endpoint (minimal SubscriptionStatus-style payload)
		payload := map[string]any{
			"resourceType": "SubscriptionStatus",
			"subscription": sub.ID,
			"status":       "active",
			"eventNumber":  1,
		}
		body, _ := json.Marshal(payload)
		req, err := http.NewRequest(http.MethodPost, sub.Endpoint, strings.NewReader(string(body)))
		if err != nil {
			continue
		}
		req.Header.Set("Content-Type", "application/fhir+json")
		http.DefaultClient.Do(req)
	}
}

func (f *FHIRTestServer) criteriaMatches(criteria string, patient map[string]any) bool {
	if criteria == "Patient" || strings.HasPrefix(criteria, "Patient?") {
		// Optional: Patient?name=SubTest → check patient name contains "SubTest"
		if idx := strings.Index(criteria, "name="); idx != -1 {
			namePart := criteria[idx+5:]
			if end := strings.IndexAny(namePart, "&"); end != -1 {
				namePart = namePart[:end]
			}
			names, _ := patient["name"].([]any)
			for _, n := range names {
				nm, _ := n.(map[string]any)
				fam, _ := nm["family"].(string)
				for _, g := range anyToStrings(nm["given"]) {
					if strings.Contains(fam, namePart) || strings.Contains(g, namePart) {
						return true
					}
				}
			}
			return false
		}
		return true
	}
	return false
}

func anyToStrings(v any) []string {
	if v == nil {
		return nil
	}
	if s, ok := v.(string); ok {
		return []string{s}
	}
	if a, ok := v.([]any); ok {
		var out []string
		for _, x := range a {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func (f *FHIRTestServer) serveSubscription(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/fhir+json")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	channel, _ := body["channel"].(map[string]any)
	endpoint, _ := channel["endpoint"].(string)
	criteria, _ := body["criteria"].(string)
	id := "sub-" + time.Now().Format("20060102150405")
	if body["id"] != nil {
		id, _ = body["id"].(string)
	}
	f.mu.Lock()
	f.subscriptions = append(f.subscriptions, fhirSubscription{ID: id, Criteria: criteria, Endpoint: endpoint})
	f.mu.Unlock()
	w.WriteHeader(http.StatusCreated)
	body["id"] = id
	body["status"] = "active"
	json.NewEncoder(w).Encode(body)
}

package healthcare

import (
	"encoding/json"
	"fmt"
)

type FHIRBundle struct {
	ResourceType string            `json:"resourceType"`
	Type         string            `json:"type"`
	Entry        []FHIRBundleEntry `json:"entry,omitempty"`
}

type FHIRBundleEntry struct {
	FullURL  string         `json:"fullUrl,omitempty"`
	Resource map[string]any `json:"resource"`
	Request  *FHIRRequest   `json:"request,omitempty"`
}

type FHIRRequest struct {
	Method string `json:"method"`
	URL    string `json:"url"`
}

func NewFHIRBundle(bundleType string) *FHIRBundle {
	return &FHIRBundle{
		ResourceType: "Bundle",
		Type:         bundleType,
	}
}

func (b *FHIRBundle) AddEntry(resource map[string]any) {
	entry := FHIRBundleEntry{Resource: resource}

	rt, _ := resource["resourceType"].(string)
	id, _ := resource["id"].(string)
	if rt != "" && id != "" {
		entry.FullURL = fmt.Sprintf("urn:uuid:%s", id)
		entry.Request = &FHIRRequest{
			Method: "POST",
			URL:    rt,
		}
	}

	b.Entry = append(b.Entry, entry)
}

func (b *FHIRBundle) ToJSON() ([]byte, error) {
	return json.MarshalIndent(b, "", "  ")
}

func BuildPatientResource(patientID, familyName, givenName string) map[string]any {
	return map[string]any{
		"resourceType": "Patient",
		"identifier": []map[string]any{
			{"value": patientID},
		},
		"name": []map[string]any{
			{
				"family": familyName,
				"given":  []string{givenName},
			},
		},
	}
}

func BuildObservationResource(patientRef, code, value, unit string) map[string]any {
	return map[string]any{
		"resourceType": "Observation",
		"status":       "final",
		"subject": map[string]any{
			"reference": patientRef,
		},
		"code": map[string]any{
			"coding": []map[string]any{
				{"code": code},
			},
		},
		"valueQuantity": map[string]any{
			"value": value,
			"unit":  unit,
		},
	}
}

func ParseFHIRBundle(data []byte) (*FHIRBundle, error) {
	var bundle FHIRBundle
	if err := json.Unmarshal(data, &bundle); err != nil {
		return nil, fmt.Errorf("parse FHIR bundle: %w", err)
	}
	if bundle.ResourceType != "Bundle" {
		return nil, fmt.Errorf("not a FHIR Bundle: resourceType=%s", bundle.ResourceType)
	}
	return &bundle, nil
}

func ExtractResources(bundle *FHIRBundle, resourceType string) []map[string]any {
	var result []map[string]any
	for _, entry := range bundle.Entry {
		rt, _ := entry.Resource["resourceType"].(string)
		if rt == resourceType || resourceType == "" {
			result = append(result, entry.Resource)
		}
	}
	return result
}

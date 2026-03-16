package healthcare

import (
	"encoding/json"
	"strings"
	"testing"
)

// --- HL7v2 tests ---

func TestNewHL7v2Builder(t *testing.T) {
	b := NewHL7v2Builder()
	if b == nil {
		t.Fatal("NewHL7v2Builder returned nil")
	}
	if got := b.Build(); got != "" {
		t.Errorf("empty builder Build() = %q, want empty string", got)
	}
}

func TestHL7v2Builder_AddSegment(t *testing.T) {
	b := NewHL7v2Builder()
	b.AddSegment("MSH", "^~\\&", "APP", "FAC")
	if len(b.segments) != 1 {
		t.Errorf("AddSegment: expected 1 segment, got %d", len(b.segments))
	}
	want := "MSH|^~\\&|APP|FAC"
	if b.segments[0] != want {
		t.Errorf("AddSegment: got %q, want %q", b.segments[0], want)
	}

	// Chaining
	b.AddSegment("PID", "1").AddSegment("PV1", "1")
	if len(b.segments) != 3 {
		t.Errorf("chained AddSegment: expected 3 segments, got %d", len(b.segments))
	}
}

func TestHL7v2Builder_AddSegment_EmptyFields(t *testing.T) {
	b := NewHL7v2Builder()
	b.AddSegment("MSH")
	if got := b.segments[0]; got != "MSH" {
		t.Errorf("AddSegment with no fields: got %q, want MSH", got)
	}
}

func TestHL7v2Builder_Build(t *testing.T) {
	b := NewHL7v2Builder()
	b.AddSegment("MSH", "a", "b").AddSegment("PID", "1")
	got := b.Build()
	want := "MSH|a|b\rPID|1"
	if got != want {
		t.Errorf("Build() = %q, want %q", got, want)
	}
}

func TestBuildACK(t *testing.T) {
	msh := map[string]any{
		"3":  "SENDING_APP",
		"4":  "SENDING_FAC",
		"5":  "RECV_APP",
		"6":  "RECV_FAC",
		"9":  "ADT^A01",
		"10": "MSG123",
	}
	ack := BuildACK(msh, "AA", "Message accepted")
	lines := strings.Split(ack, "\r")
	if len(lines) < 2 {
		t.Fatalf("BuildACK: expected at least 2 segments, got %d", len(lines))
	}
	if !strings.HasPrefix(lines[0], "MSH|") {
		t.Errorf("BuildACK: first segment should be MSH, got %q", lines[0])
	}
	if !strings.HasPrefix(lines[1], "MSA|AA|MSG123|") {
		t.Errorf("BuildACK: MSA should have AA, MSG123; got %q", lines[1])
	}
	// Swapped sending/receiving
	if !strings.Contains(lines[0], "RECV_APP") || !strings.Contains(lines[0], "RECV_FAC") {
		t.Error("BuildACK: MSH should contain receiving app/fac")
	}
	if !strings.Contains(lines[0], "SENDING_APP") || !strings.Contains(lines[0], "SENDING_FAC") {
		t.Error("BuildACK: MSH should contain sending app/fac")
	}
}

func TestBuildACK_MSHWithMapField(t *testing.T) {
	msh := map[string]any{
		"3":  map[string]any{"1": "APP"},
		"4":  "FAC",
		"5":  "RAPP",
		"6":  "RFAC",
		"9":  "ADT^A01",
		"10": "ID1",
	}
	ack := BuildACK(msh, "AE", "Error")
	if !strings.Contains(ack, "APP") {
		t.Error("BuildACK with map field: should extract value from map")
	}
	if !strings.Contains(ack, "MSA|AE|") {
		t.Errorf("BuildACK: MSA should have AE; got %q", ack)
	}
}

func TestBuildACK_EmptyMSH(t *testing.T) {
	msh := map[string]any{}
	ack := BuildACK(msh, "AA", "OK")
	if ack == "" {
		t.Error("BuildACK with empty MSH should still produce output")
	}
	lines := strings.Split(ack, "\r")
	if len(lines) < 2 {
		t.Fatalf("BuildACK empty MSH: expected 2 segments, got %d", len(lines))
	}
}

func TestBuildNACK(t *testing.T) {
	msh := map[string]any{"3": "A", "4": "F", "5": "R", "6": "RF", "9": "ADT^A01", "10": "X"}
	nack := BuildNACK(msh, "AR", "Application reject")
	if !strings.Contains(nack, "MSA|AR|") {
		t.Errorf("BuildNACK: expected MSA|AR|; got %q", nack)
	}
	if !strings.Contains(nack, "Application reject") {
		t.Error("BuildNACK: should include error message")
	}
}

func TestParseHL7Path(t *testing.T) {
	msg := map[string]any{
		"PID": map[string]any{
			"5": "Doe^John",
			"3": map[string]any{"1": "ID123", "2": "MR"},
		},
	}

	// seg.field
	got, err := ParseHL7Path(msg, "PID.5")
	if err != nil {
		t.Fatalf("ParseHL7Path PID.5: %v", err)
	}
	if got != "Doe^John" {
		t.Errorf("ParseHL7Path PID.5 = %q, want Doe^John", got)
	}

	// seg.field.component
	got, err = ParseHL7Path(msg, "PID.3.1")
	if err != nil {
		t.Fatalf("ParseHL7Path PID.3.1: %v", err)
	}
	if got != "ID123" {
		t.Errorf("ParseHL7Path PID.3.1 = %q, want ID123", got)
	}
}

func TestParseHL7Path_InvalidPath(t *testing.T) {
	msg := map[string]any{"PID": map[string]any{"5": "x"}}
	msgWithMap := map[string]any{"PID": map[string]any{"5": map[string]any{"1": "a"}}} // map without key "99"

	tests := []struct {
		path string
		msg  map[string]any
		want string
	}{
		{"", msg, "invalid HL7 path"},
		{"PID", msg, "invalid HL7 path"},
		{"NONE.5", msg, "segment NONE not found"},
		{"PID.99", msg, "field PID.99 not found"},
		{"PID.5.99", msgWithMap, "component PID.5.99 not found"},
	}

	for _, tt := range tests {
		_, err := ParseHL7Path(tt.msg, tt.path)
		if err == nil {
			t.Errorf("ParseHL7Path(%q): expected error containing %q", tt.path, tt.want)
			continue
		}
		if !strings.Contains(err.Error(), tt.want) {
			t.Errorf("ParseHL7Path(%q): error %v does not contain %q", tt.path, err, tt.want)
		}
	}
}

func TestParseHL7Path_SegmentNotMap(t *testing.T) {
	msg := map[string]any{"PID": "not-a-map"}
	_, err := ParseHL7Path(msg, "PID.5")
	if err == nil {
		t.Fatal("ParseHL7Path: expected error when segment is not a map")
	}
	if !strings.Contains(err.Error(), "not a map") {
		t.Errorf("error should mention 'not a map': %v", err)
	}
}

func TestParseHL7Path_FieldNotMap_ReturnsString(t *testing.T) {
	msg := map[string]any{"PID": map[string]any{"5": "simple"}}
	got, err := ParseHL7Path(msg, "PID.5.1")
	if err != nil {
		t.Fatalf("ParseHL7Path PID.5.1 with non-map field: %v", err)
	}
	if got != "simple" {
		t.Errorf("ParseHL7Path PID.5.1 = %q, want simple (field is string, component ignored)", got)
	}
}

// --- FHIR tests ---

func TestNewFHIRBundle(t *testing.T) {
	b := NewFHIRBundle("transaction")
	if b == nil {
		t.Fatal("NewFHIRBundle returned nil")
	}
	if b.ResourceType != "Bundle" {
		t.Errorf("ResourceType = %q, want Bundle", b.ResourceType)
	}
	if b.Type != "transaction" {
		t.Errorf("Type = %q, want transaction", b.Type)
	}
}

func TestFHIRBundle_AddEntry(t *testing.T) {
	b := NewFHIRBundle("document")
	patient := map[string]any{
		"resourceType": "Patient",
		"id":           "p1",
		"name":         []map[string]any{{"family": "Doe"}},
	}
	b.AddEntry(patient)
	if len(b.Entry) != 1 {
		t.Fatalf("AddEntry: expected 1 entry, got %d", len(b.Entry))
	}
	e := b.Entry[0]
	if e.FullURL != "urn:uuid:p1" {
		t.Errorf("FullURL = %q, want urn:uuid:p1", e.FullURL)
	}
	if e.Request == nil || e.Request.Method != "POST" || e.Request.URL != "Patient" {
		t.Errorf("Request = %+v, want POST Patient", e.Request)
	}
}

func TestFHIRBundle_AddEntry_NoResourceTypeOrID(t *testing.T) {
	b := NewFHIRBundle("document")
	resource := map[string]any{"data": "value"}
	b.AddEntry(resource)
	if len(b.Entry) != 1 {
		t.Fatalf("AddEntry: expected 1 entry, got %d", len(b.Entry))
	}
	e := b.Entry[0]
	if e.FullURL != "" {
		t.Errorf("FullURL should be empty when no resourceType+id: %q", e.FullURL)
	}
	if e.Request != nil {
		t.Errorf("Request should be nil when no resourceType+id: %+v", e.Request)
	}
}

func TestFHIRBundle_ToJSON(t *testing.T) {
	b := NewFHIRBundle("collection")
	b.AddEntry(map[string]any{"resourceType": "Patient", "id": "1"})
	data, err := b.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	var decoded FHIRBundle
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("ToJSON output not valid JSON: %v", err)
	}
	if decoded.ResourceType != "Bundle" || decoded.Type != "collection" {
		t.Errorf("decoded bundle: ResourceType=%q Type=%q", decoded.ResourceType, decoded.Type)
	}
}

func TestBuildPatientResource(t *testing.T) {
	r := BuildPatientResource("P001", "Doe", "John")
	if r["resourceType"] != "Patient" {
		t.Errorf("resourceType = %v, want Patient", r["resourceType"])
	}
	ids, _ := r["identifier"].([]map[string]any)
	if len(ids) == 0 || ids[0]["value"] != "P001" {
		t.Errorf("identifier: %+v", r["identifier"])
	}
	names, _ := r["name"].([]map[string]any)
	if len(names) == 0 || names[0]["family"] != "Doe" {
		t.Errorf("name: %+v", r["name"])
	}
	given, _ := names[0]["given"].([]string)
	if len(given) == 0 || given[0] != "John" {
		t.Errorf("given: %+v", names[0]["given"])
	}
}

func TestBuildObservationResource(t *testing.T) {
	r := BuildObservationResource("Patient/123", "8867-4", "120", "mmHg")
	if r["resourceType"] != "Observation" {
		t.Errorf("resourceType = %v, want Observation", r["resourceType"])
	}
	if r["status"] != "final" {
		t.Errorf("status = %v, want final", r["status"])
	}
	subj, _ := r["subject"].(map[string]any)
	if subj["reference"] != "Patient/123" {
		t.Errorf("subject.reference = %v", subj["reference"])
	}
	vq, _ := r["valueQuantity"].(map[string]any)
	if vq["value"] != "120" || vq["unit"] != "mmHg" {
		t.Errorf("valueQuantity = %+v", vq)
	}
}

func TestParseFHIRBundle(t *testing.T) {
	data := []byte(`{"resourceType":"Bundle","type":"searchset","entry":[]}`)
	b, err := ParseFHIRBundle(data)
	if err != nil {
		t.Fatalf("ParseFHIRBundle: %v", err)
	}
	if b.ResourceType != "Bundle" || b.Type != "searchset" {
		t.Errorf("bundle: ResourceType=%q Type=%q", b.ResourceType, b.Type)
	}
}

func TestParseFHIRBundle_InvalidJSON(t *testing.T) {
	_, err := ParseFHIRBundle([]byte(`{invalid`))
	if err == nil {
		t.Fatal("ParseFHIRBundle: expected error for invalid JSON")
	}
	if !strings.Contains(err.Error(), "parse FHIR bundle") {
		t.Errorf("error should mention parse: %v", err)
	}
}

func TestParseFHIRBundle_NotBundle(t *testing.T) {
	data := []byte(`{"resourceType":"Patient","id":"1"}`)
	_, err := ParseFHIRBundle(data)
	if err == nil {
		t.Fatal("ParseFHIRBundle: expected error when not a Bundle")
	}
	if !strings.Contains(err.Error(), "not a FHIR Bundle") {
		t.Errorf("error should mention not a FHIR Bundle: %v", err)
	}
}

func TestExtractResources(t *testing.T) {
	b := &FHIRBundle{
		Entry: []FHIRBundleEntry{
			{Resource: map[string]any{"resourceType": "Patient", "id": "1"}},
			{Resource: map[string]any{"resourceType": "Observation", "id": "2"}},
			{Resource: map[string]any{"resourceType": "Patient", "id": "3"}},
		},
	}

	patients := ExtractResources(b, "Patient")
	if len(patients) != 2 {
		t.Errorf("ExtractResources Patient: got %d, want 2", len(patients))
	}

	obs := ExtractResources(b, "Observation")
	if len(obs) != 1 {
		t.Errorf("ExtractResources Observation: got %d, want 1", len(obs))
	}

	all := ExtractResources(b, "")
	if len(all) != 3 {
		t.Errorf("ExtractResources empty type: got %d, want 3", len(all))
	}

	none := ExtractResources(b, "Device")
	if len(none) != 0 {
		t.Errorf("ExtractResources Device: got %d, want 0", len(none))
	}
}

func TestExtractResources_EmptyBundle(t *testing.T) {
	b := &FHIRBundle{Entry: nil}
	got := ExtractResources(b, "Patient")
	if got != nil {
		t.Errorf("ExtractResources empty bundle: got %v, want nil or empty", got)
	}
}

// --- X12 tests ---

func TestParseX12_TooShort(t *testing.T) {
	_, err := ParseX12("short")
	if err == nil {
		t.Fatal("ParseX12: expected error for message too short")
	}
	if !strings.Contains(err.Error(), "too short") {
		t.Errorf("error should mention too short: %v", err)
	}
}

func TestParseX12(t *testing.T) {
	// Minimal valid X12: ISA (106 chars) + segment separator + ST
	raw := "ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       *230101*1200*^*00501*000000001*0*P*:~ST*837*0001~GS*HC*S*R*20230101*1200*1*X*005010X222A1~"
	tx, err := ParseX12(raw)
	if err != nil {
		t.Fatalf("ParseX12: %v", err)
	}
	if len(tx.Segments) < 2 {
		t.Errorf("ParseX12: expected at least 2 segments, got %d", len(tx.Segments))
	}
	if tx.TransactionSet != "837" {
		t.Errorf("TransactionSet = %q, want 837", tx.TransactionSet)
	}
	if tx.Version != "005010X222A1" {
		t.Errorf("Version = %q, want 005010X222A1", tx.Version)
	}
}

func TestParseX12_ElementSeparator(t *testing.T) {
	raw := "ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       *230101*1200*^*00501*000000001*0*P*:~ST*850*0001~"
	tx, err := ParseX12(raw)
	if err != nil {
		t.Fatalf("ParseX12: %v", err)
	}
	stSegs := tx.FindSegments("ST")
	if len(stSegs) == 0 {
		t.Fatal("FindSegments ST: no segments found")
	}
	if stSegs[0].Elements[0] != "850" {
		t.Errorf("ST first element = %q, want 850", stSegs[0].Elements[0])
	}
}

func TestParseX12_EmptyPartsSkipped(t *testing.T) {
	// Double segment separator and whitespace-only parts get skipped
	raw := "ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       *230101*1200*^*00501*000000001*0*P*:~  ~ST*837*0001~"
	tx, err := ParseX12(raw)
	if err != nil {
		t.Fatalf("ParseX12: %v", err)
	}
	stSegs := tx.FindSegments("ST")
	if len(stSegs) == 0 {
		t.Fatal("empty/whitespace parts should be skipped, ST should exist")
	}
}

func TestX12Transaction_FindSegments(t *testing.T) {
	raw := "ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       *230101*1200*^*00501*000000001*0*P*:~ST*837*0001~NM1*41*2*Doe*John~NM1*41*2*Smith*Jane~"
	tx, err := ParseX12(raw)
	if err != nil {
		t.Fatalf("ParseX12: %v", err)
	}

	nm1 := tx.FindSegments("NM1")
	if len(nm1) != 2 {
		t.Errorf("FindSegments NM1: got %d, want 2", len(nm1))
	}

	none := tx.FindSegments("XYZ")
	if len(none) != 0 {
		t.Errorf("FindSegments XYZ: got %d, want 0", len(none))
	}
}

func TestX12Transaction_GetElement(t *testing.T) {
	raw := "ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       *230101*1200*^*00501*000000001*0*P*:~ST*837*0001~"
	tx, err := ParseX12(raw)
	if err != nil {
		t.Fatalf("ParseX12: %v", err)
	}

	if got := tx.GetElement("ST", 0); got != "837" {
		t.Errorf("GetElement ST 0 = %q, want 837", got)
	}
	if got := tx.GetElement("ST", 1); got != "0001" {
		t.Errorf("GetElement ST 1 = %q, want 0001", got)
	}
	if got := tx.GetElement("ST", 99); got != "" {
		t.Errorf("GetElement ST 99 (out of range) = %q, want empty", got)
	}
	if got := tx.GetElement("NONE", 0); got != "" {
		t.Errorf("GetElement NONE 0 = %q, want empty", got)
	}
}

func TestX12Transaction_Serialize(t *testing.T) {
	raw := "ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       *230101*1200*^*00501*000000001*0*P*:~ST*837*0001~"
	tx, err := ParseX12(raw)
	if err != nil {
		t.Fatalf("ParseX12: %v", err)
	}
	ser := tx.Serialize("*", "~")
	if !strings.Contains(ser, "ST*837*0001") {
		t.Errorf("Serialize: expected ST*837*0001 in output: %q", ser)
	}
	if !strings.Contains(ser, "~") {
		t.Error("Serialize: expected segment separator in output")
	}
}

func TestX12Transaction_Serialize_Empty(t *testing.T) {
	tx := &X12Transaction{Segments: nil}
	got := tx.Serialize("*", "~")
	if got != "" {
		t.Errorf("Serialize empty: got %q, want empty", got)
	}
}

// --- CDA tests ---

const sampleCDA = `<?xml version="1.0" encoding="UTF-8"?>
<ClinicalDocument xmlns="urn:hl7-org:v3">
  <id root="2.16.840.1.113883.19" extension="doc123"/>
  <code code="34133-9" codeSystem="2.16.840.1.113883.6.1" displayName="Summarization of Episode Note"/>
  <title>Clinical Document</title>
  <effectiveTime value="20230101120000"/>
  <recordTarget>
    <patientRole>
      <id root="2.16.840.1.113883.19.5" extension="PAT001"/>
      <patient>
        <name>
          <given>John</given>
          <family>Doe</family>
        </name>
      </patient>
    </patientRole>
  </recordTarget>
  <component>
    <structuredBody>
      <component>
        <section>
          <code code="10164-2" codeSystem="2.16.840.1.113883.6.1"/>
          <title>History of Present Illness</title>
          <text>Patient presents with...</text>
        </section>
      </component>
    </structuredBody>
  </component>
</ClinicalDocument>`

func TestParseCDA(t *testing.T) {
	doc, err := ParseCDA([]byte(sampleCDA))
	if err != nil {
		t.Fatalf("ParseCDA: %v", err)
	}
	if doc == nil {
		t.Fatal("ParseCDA returned nil doc")
	}
	if doc.Title != "Clinical Document" {
		t.Errorf("Title = %q, want Clinical Document", doc.Title)
	}
	if doc.ID == nil || doc.ID.Extension != "doc123" {
		t.Errorf("ID: %+v", doc.ID)
	}
}

func TestParseCDA_InvalidXML(t *testing.T) {
	_, err := ParseCDA([]byte(`<ClinicalDocument><unclosed`))
	if err == nil {
		t.Fatal("ParseCDA: expected error for invalid XML")
	}
	if !strings.Contains(err.Error(), "parse CDA document") {
		t.Errorf("error should mention parse: %v", err)
	}
}

func TestCDADocument_GetPatientName(t *testing.T) {
	doc, err := ParseCDA([]byte(sampleCDA))
	if err != nil {
		t.Fatalf("ParseCDA: %v", err)
	}
	given, family := doc.GetPatientName()
	if given != "John" || family != "Doe" {
		t.Errorf("GetPatientName = %q, %q; want John, Doe", given, family)
	}
}

func TestCDADocument_GetPatientName_NilChain(t *testing.T) {
	doc := &CDADocument{}
	given, family := doc.GetPatientName()
	if given != "" || family != "" {
		t.Errorf("GetPatientName nil doc: got %q, %q", given, family)
	}

	doc = &CDADocument{RecordTarget: &CDARecordTarget{}}
	given, family = doc.GetPatientName()
	if given != "" || family != "" {
		t.Errorf("GetPatientName nil PatientRole: got %q, %q", given, family)
	}

	doc = &CDADocument{
		RecordTarget: &CDARecordTarget{
			PatientRole: &CDAPatientRole{Patient: &CDAPatient{}},
		},
	}
	given, family = doc.GetPatientName()
	if given != "" || family != "" {
		t.Errorf("GetPatientName nil Name: got %q, %q", given, family)
	}
}

func TestCDADocument_GetPatientID(t *testing.T) {
	doc, err := ParseCDA([]byte(sampleCDA))
	if err != nil {
		t.Fatalf("ParseCDA: %v", err)
	}
	id := doc.GetPatientID()
	if id != "PAT001" {
		t.Errorf("GetPatientID = %q, want PAT001", id)
	}
}

func TestCDADocument_GetPatientID_NilChain(t *testing.T) {
	doc := &CDADocument{}
	if got := doc.GetPatientID(); got != "" {
		t.Errorf("GetPatientID nil doc: got %q", got)
	}

	doc = &CDADocument{RecordTarget: &CDARecordTarget{}}
	if got := doc.GetPatientID(); got != "" {
		t.Errorf("GetPatientID nil PatientRole: got %q", got)
	}

	doc = &CDADocument{
		RecordTarget: &CDARecordTarget{
			PatientRole: &CDAPatientRole{ID: nil},
		},
	}
	if got := doc.GetPatientID(); got != "" {
		t.Errorf("GetPatientID nil ID: got %q", got)
	}
}

func TestCDADocument_ToXML(t *testing.T) {
	doc, err := ParseCDA([]byte(sampleCDA))
	if err != nil {
		t.Fatalf("ParseCDA: %v", err)
	}
	data, err := doc.ToXML()
	if err != nil {
		t.Fatalf("ToXML: %v", err)
	}
	if len(data) == 0 {
		t.Error("ToXML: returned empty")
	}
	// Round-trip
	doc2, err := ParseCDA(data)
	if err != nil {
		t.Fatalf("ParseCDA round-trip: %v", err)
	}
	if doc2.GetPatientID() != doc.GetPatientID() {
		t.Error("ToXML round-trip: patient ID mismatch")
	}
}

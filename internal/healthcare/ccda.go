package healthcare

import (
	"encoding/xml"
	"fmt"
)

type CDADocument struct {
	XMLName        xml.Name        `xml:"ClinicalDocument"`
	ID             *CDAID          `xml:"id"`
	Code           *CDACode        `xml:"code"`
	Title          string          `xml:"title"`
	EffectiveTime  *CDATime        `xml:"effectiveTime"`
	RecordTarget   *CDARecordTarget `xml:"recordTarget"`
	Components     []CDAComponent  `xml:"component"`
}

type CDAID struct {
	Root      string `xml:"root,attr"`
	Extension string `xml:"extension,attr"`
}

type CDACode struct {
	Code           string `xml:"code,attr"`
	CodeSystem     string `xml:"codeSystem,attr"`
	DisplayName    string `xml:"displayName,attr"`
	CodeSystemName string `xml:"codeSystemName,attr"`
}

type CDATime struct {
	Value string `xml:"value,attr"`
}

type CDARecordTarget struct {
	PatientRole *CDAPatientRole `xml:"patientRole"`
}

type CDAPatientRole struct {
	ID      *CDAID      `xml:"id"`
	Patient *CDAPatient `xml:"patient"`
}

type CDAPatient struct {
	Name      *CDAName `xml:"name"`
	Gender    *CDACode `xml:"administrativeGenderCode"`
	BirthTime *CDATime `xml:"birthTime"`
}

type CDAName struct {
	Given  string `xml:"given"`
	Family string `xml:"family"`
}

type CDAComponent struct {
	Section *CDASection `xml:"structuredBody>component>section"`
}

type CDASection struct {
	Code  *CDACode `xml:"code"`
	Title string   `xml:"title"`
	Text  string   `xml:"text"`
}

func ParseCDA(data []byte) (*CDADocument, error) {
	var doc CDADocument
	if err := xml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse CDA document: %w", err)
	}
	return &doc, nil
}

func (doc *CDADocument) GetPatientName() (given, family string) {
	if doc.RecordTarget != nil &&
		doc.RecordTarget.PatientRole != nil &&
		doc.RecordTarget.PatientRole.Patient != nil &&
		doc.RecordTarget.PatientRole.Patient.Name != nil {
		return doc.RecordTarget.PatientRole.Patient.Name.Given,
			doc.RecordTarget.PatientRole.Patient.Name.Family
	}
	return "", ""
}

func (doc *CDADocument) GetPatientID() string {
	if doc.RecordTarget != nil &&
		doc.RecordTarget.PatientRole != nil &&
		doc.RecordTarget.PatientRole.ID != nil {
		return doc.RecordTarget.PatientRole.ID.Extension
	}
	return ""
}

func (doc *CDADocument) ToXML() ([]byte, error) {
	return xml.MarshalIndent(doc, "", "  ")
}

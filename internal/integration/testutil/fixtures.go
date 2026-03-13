//go:build integration

package testutil

const HL7v2_ADT_A01 = "MSH|^~\\&|LABSYS|HOSPITAL|FHIRSYS|CLOUD|20230615120000||ADT^A01|MSG001|P|2.5\r" +
	"PID|1||MRN12345||Smith^Jane^M||19850301|F|||123 Main St^^Springfield^IL^62704\r" +
	"PV1|1|I|ICU^101^A|E|||1234^Jones^Robert^^^Dr||||||||||||||V001\r"

const HL7v2_ADT_A01_2 = "MSH|^~\\&|LABSYS|HOSPITAL|FHIRSYS|CLOUD|20230615120100||ADT^A01|MSG002|P|2.5\r" +
	"PID|1||MRN67890||Doe^John^Q||19900715|M|||456 Oak Ave^^Chicago^IL^60601\r" +
	"PV1|1|O|ER^201^B|U|||5678^Brown^Alice^^^Dr||||||||||||||V002\r"

const HL7v2_ORM_O01 = "MSH|^~\\&|LAB|FACILITY|ORDER|SYSTEM|20230616||ORM^O01|CTRL003|P|2.5\r" +
	"PID|1||MRN11111||Johnson^Michael^A||19781225|M\r" +
	"ORC|NW|ORD001||||||20230616\r" +
	"OBR|1|ORD001||87880^Strep A Rapid Test^CPT\r"

const JSONPatientRegistration = `{
	"patientId": "P001",
	"firstName": "Alice",
	"lastName": "Brown",
	"dob": "1990-05-15",
	"mrn": "MRN-001",
	"facility": "Springfield General"
}`

const FHIRPatientBundle = `{
	"resourceType": "Bundle",
	"type": "transaction",
	"entry": [{
		"resource": {
			"resourceType": "Patient",
			"identifier": [{"system": "urn:oid:2.16.840.1.113883.2.1", "value": "MRN12345"}],
			"name": [{"family": "Smith", "given": ["Jane", "M"]}],
			"gender": "female",
			"birthDate": "1985-03-01"
		},
		"request": {"method": "POST", "url": "Patient"}
	}]
}`

const TransformerHL7ToFHIR = `
exports.transform = function transform(msg, ctx) {
	var pid = msg.body.PID || {};
	var nameField = pid["5"] || {};
	var family = nameField["1"] || "Unknown";
	var given = nameField["2"] || "Unknown";
	var mrn = pid["3"] || "000";

	return {
		body: {
			resourceType: "Bundle",
			type: "transaction",
			entry: [{
				resource: {
					resourceType: "Patient",
					identifier: [{ system: "urn:oid:2.16.840.1.113883.2.1", value: mrn }],
					name: [{ family: family, given: [given] }],
					active: true
				},
				request: { method: "POST", url: "Patient" }
			}]
		},
	};
};`

const TransformerPassthrough = `
exports.transform = function transform(msg, ctx) {
	return { body: msg.body };
};`

const TransformerJSONEnrich = `
exports.transform = function transform(msg, ctx) {
	var data = msg.body;
	if (typeof data === "string") {
		try { data = JSON.parse(data); } catch(e) {}
	}
	data.processedAt = new Date().toISOString();
	data.channelId = ctx.channelId;
	data.transport = msg.transport;
	return { body: data };
};`

const ValidatorHL7 = `
exports.validate = function validate(msg, ctx) {
	var b = msg.body;
	if (!b || !b.MSH) return false;
	var msgType = b.MSH["8"];
	if (!msgType) return false;
	return true;
};`

const ValidatorNonEmpty = `
exports.validate = function validate(msg, ctx) {
	return msg.body !== null && msg.body !== undefined && msg.body !== "";
};`

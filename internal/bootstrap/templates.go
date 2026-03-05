package bootstrap

import "fmt"

var projectDirectories = []string{
	"channels/sample-channel",
	"lib",
}

var projectFiles = map[string]string{
	"intu.yaml":                              intuYAML,
	"intu.dev.yaml":                          intuDevYAML,
	"intu.prod.yaml":                         intuProdYAML,
	".env":                                   dotEnv,
	"channels/sample-channel/channel.yaml":   fmt.Sprintf(channelYAMLTpl, "sample-channel"),
	"channels/sample-channel/transformer.ts": transformerTSTpl,
	"channels/sample-channel/validator.ts":   validatorTSTpl,
	"lib/index.ts":                           libIndexTS,
	"package.json":                           packageJSON,
	"tsconfig.json":                          tsConfigJSON,
	"README.md":                              projectREADME,
}

const intuYAML = `runtime:
  name: intu
  profile: dev
  log_level: info
  storage:
    driver: memory
    postgres_dsn: ${INTU_POSTGRES_DSN}

channels_dir: channels

destinations:
  kafka-output:
    type: kafka
    kafka:
      brokers:
        - ${INTU_KAFKA_BROKER}
      topic: output-topic

kafka:
  brokers:
    - ${INTU_KAFKA_BROKER}
`

const intuDevYAML = `runtime:
  profile: dev
  log_level: debug

kafka:
  client_id: intu-dev
`

const intuProdYAML = `runtime:
  profile: prod
  log_level: info
  storage:
    driver: postgres

kafka:
  client_id: intu-prod
`

const dotEnv = `INTU_PROFILE=dev
INTU_KAFKA_BROKER=localhost:9092
INTU_POSTGRES_DSN=postgres://postgres:postgres@localhost:5432/intu?sslmode=disable
`

const channelYAMLTpl = `id: %s
enabled: true

listener:
  type: http
  http:
    port: 8080

validator:
  runtime: node
  entrypoint: validator.ts

transformer:
  runtime: node
  entrypoint: transformer.ts

destinations:
  - kafka-output
`

const transformerTSTpl = `export function transform(msg: unknown, ctx: { channelId: string; correlationId: string }): unknown {
  return {
    ...(msg as object),
    processedAt: new Date().toISOString(),
    source: ctx.channelId,
  };
}
`

const validatorTSTpl = `export function validate(msg: unknown): void {
  if (!msg || typeof msg !== "object") {
    throw new Error("Message must be an object.");
  }

  if (!("patientId" in msg)) {
    throw new Error("Missing required field: patientId.");
  }
}
`

// channelFiles returns the file map for a channel (used by BootstrapChannel).
func channelFiles(channelName string) map[string]string {
	return map[string]string{
		"channels/" + channelName + "/channel.yaml":   fmt.Sprintf(channelYAMLTpl, channelName),
		"channels/" + channelName + "/transformer.ts": transformerTSTpl,
		"channels/" + channelName + "/validator.ts":  validatorTSTpl,
	}
}

const libIndexTS = `/**
 * Shared library for intu transformers.
 * Import from channel transformers:
 *   import { formatTimestamp } from "../../lib/index";
 */

export function formatTimestamp(date: Date): string {
  return date.toISOString();
}

export function generateId(): string {
  return Math.random().toString(36).substring(2) + Date.now().toString(36);
}
`

const packageJSON = `{
  "name": "intu-channel-runtime",
  "private": true,
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "check": "tsc --noEmit -p tsconfig.json"
  },
  "devDependencies": {
    "typescript": "^5.6.0"
  }
}
`

const tsConfigJSON = `{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "strict": true,
    "esModuleInterop": true,
    "forceConsistentCasingInFileNames": true,
    "skipLibCheck": true,
    "rootDir": ".",
    "outDir": "dist"
  },
  "include": ["channels/**/*.ts", "lib/**/*.ts"]
}
`

const projectREADME = `# intu Project

Bootstrapped project for the [intu](https://intu.dev) interoperability framework.

## Quick Start

1. Review and update configuration files:
   - intu.yaml
   - intu.dev.yaml
   - intu.prod.yaml
   - .env
2. Install transformer runtime dependencies:
   - npm install
3. Build TypeScript transformers:
   - intu build --dir .
4. Validate configuration:
   - intu validate --dir .
5. Run the framework once serve is implemented:
   - intu serve

## Structure

- channels/sample-channel/channel.yaml: sample channel definition.
- channels/sample-channel/transformer.ts: pure transformer (JSON in, JSON out).
- channels/sample-channel/validator.ts: sample validator.
- lib/index.ts: shared utility functions for transformers.

## Add a Channel

  intu c my-channel --dir .
  intu channel add my-channel --dir .

## JSON Schemas (IDE & AI Assistance)

intu provides JSON schemas for configuration files to enable autocompletion,
validation, and AI-assisted configuration generation.

- Channel schema: https://intu.dev/schema/channel.schema.json
- Profile schema: https://intu.dev/schema/profile.schema.json

### VS Code Setup

Add to your .vscode/settings.json:

  {
    "yaml.schemas": {
      "https://intu.dev/schema/channel.schema.json": "channels/*/channel.yaml",
      "https://intu.dev/schema/profile.schema.json": ["intu.yaml", "intu.*.yaml"]
    }
  }

### AI Assistants

When using AI coding assistants, reference the schemas above to help
generate valid intu configuration. The schemas define all valid
properties, types, and descriptions for channel.yaml and intu.yaml files.

## Documentation

Full documentation: https://intu.dev/documentation/
`

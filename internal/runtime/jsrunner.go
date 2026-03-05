package runtime

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/dop251/goja"
)

type JSRunner interface {
	Call(fn string, entrypoint string, args ...any) (any, error)
	Close() error
}

type GojaRunner struct {
	mu sync.Mutex
	vm *goja.Runtime
}

func NewGojaRunner() *GojaRunner {
	vm := goja.New()
	vm.SetFieldNameMapper(goja.TagFieldNameMapper("json", true))
	return &GojaRunner{vm: vm}
}

func (r *GojaRunner) Call(fn string, entrypoint string, args ...any) (any, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	src, err := os.ReadFile(entrypoint)
	if err != nil {
		return nil, fmt.Errorf("read entrypoint %s: %w", entrypoint, err)
	}

	code := convertESModuleToCJS(string(src))

	wrapped := fmt.Sprintf(`(function(exports){%s; return exports;})({})`, code)
	val, err := r.vm.RunString(wrapped)
	if err != nil {
		return nil, fmt.Errorf("execute JS %s: %w", entrypoint, err)
	}

	obj := val.ToObject(r.vm)
	callable, ok := goja.AssertFunction(obj.Get(fn))
	if !ok {
		return nil, fmt.Errorf("function %q not found in %s", fn, entrypoint)
	}

	jsArgs := make([]goja.Value, len(args))
	for i, a := range args {
		jsArgs[i] = r.vm.ToValue(a)
	}

	result, err := callable(goja.Undefined(), jsArgs...)
	if err != nil {
		return nil, fmt.Errorf("call %s in %s: %w", fn, entrypoint, err)
	}

	return result.Export(), nil
}

func convertESModuleToCJS(src string) string {
	lines := strings.Split(src, "\n")
	var result []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "import ") {
			continue
		}
		if strings.HasPrefix(trimmed, "export function ") {
			line = strings.Replace(line, "export function ", "exports.", 1)
			idx := strings.Index(line, "(")
			if idx > 0 {
				fnName := strings.TrimSpace(line[len("exports."):idx])
				line = "exports." + fnName + " = function " + fnName + line[idx:]
			}
		} else if strings.HasPrefix(trimmed, "export async function ") {
			line = strings.Replace(line, "export async function ", "exports.", 1)
			idx := strings.Index(line, "(")
			if idx > 0 {
				fnName := strings.TrimSpace(line[len("exports."):idx])
				line = "exports." + fnName + " = async function " + fnName + line[idx:]
			}
		} else if strings.HasPrefix(trimmed, "export ") {
			line = strings.Replace(line, "export ", "exports.", 1)
		}
		result = append(result, line)
	}
	return strings.Join(result, "\n")
}

func (r *GojaRunner) Close() error {
	return nil
}

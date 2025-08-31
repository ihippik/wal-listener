package transformer

import (
	"errors"
	"fmt"
	"sync"

	"github.com/dop251/goja"
)

type JS struct {
	runtime *goja.Runtime
}

type JSPool struct {
	pool sync.Pool
}

func NewJSPool() *JSPool {
	return &JSPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &JS{
					runtime: goja.New(),
				}
			},
		},
	}
}

func (p *JSPool) Transform(
	script string,
	data, oldData map[string]any,
	action string,
) (map[string]any, error) {
	js := p.pool.Get().(*JS)
	defer p.pool.Put(js)

	return js.transform(script, data, oldData, action)
}

// WARNING: This is not thread-safe! Use NewJSPool() for production code.
func NewJS() *JS {
	return &JS{
		runtime: goja.New(),
	}
}

var deepCopyScript string = `
      const data = JSON.parse(JSON.stringify(data));
      const oldData = JSON.parse(JSON.stringify(oldData));
`

// Transform method for individual JS instances (for backward compatibility)
// WARNING: Not thread-safe!
func (j *JS) Transform(
	script string,
	data, oldData map[string]any,
	action string,
) (map[string]any, error) {
	return j.transform(script, data, oldData, action)
}

// transform is the internal implementation used by both JS and JSPool
func (j *JS) transform(
	script string,
	data, oldData map[string]any,
	action string,
) (map[string]any, error) {
	wrappedScript := fmt.Sprintf(`
			%s
			
			function __walSpecialTransform(data, oldData, action) {
					const dataCopy = JSON.parse(JSON.stringify(data));
					const oldDataCopy = JSON.parse(JSON.stringify(oldData));

					return transform(dataCopy, oldDataCopy, action);
			}
	`, script)
	_, err := j.runtime.RunString(wrappedScript)
	if err != nil {
		return nil, err
	}

	transform, ok := goja.AssertFunction(j.runtime.Get("__walSpecialTransform"))
	if !ok {
		return nil, errors.New("transform function could not be found")
	}

	result, err := transform(
		j.runtime.ToValue(data),
		j.runtime.ToValue(oldData),
		j.runtime.ToValue(action),
	)
	if err != nil {
		return nil, err
	}

	var transformedData map[string]any
	err = j.runtime.ExportTo(result, &transformedData)
	if err != nil {
		return nil, err
	}

	return transformedData, nil
}

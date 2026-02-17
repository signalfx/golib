package distconf

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	"github.com/signalfx/golib/v3/log"
	"github.com/stretchr/testify/assert"
)

func TestInvalidYamlConfigPath(t *testing.T) {
	_, err := Yaml("/asdfdsaf/asdf/dsfad/sdfsa/fdsa/fasd/dsfa/sdfa/")
	assert.Error(t, err)
}

func TestYamlConf(t *testing.T) {
	// Create a temporary YAML file
	file, err := ioutil.TempFile("", "TestYamlConf")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(`val1: abc`), 0))

	y, err := YamlLoader(file.Name()).Get()
	assert.NoError(t, err)

	b, err := y.Get("some_non_existent_key")
	assert.NoError(t, err)
	assert.Nil(t, b)

	v, err := y.Get("val1")
	assert.NoError(t, err)
	assert.Equal(t, []byte("abc"), v)
}

func TestYamlArrays(t *testing.T) {
	// Create a temporary YAML file with an array
	file, err := ioutil.TempFile("", "TestYamlArrays")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())
	yamlContent := `
items:
  - item1
  - item2
  - item3
numbers:
  - 1
  - 2
  - 3
`
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(yamlContent), 0))

	y, err := YamlLoader(file.Name()).Get()
	assert.NoError(t, err)

	// Test string array
	v, err := y.Get("items")
	assert.NoError(t, err)
	assert.NotNil(t, v)

	var items []string
	err = json.Unmarshal(v, &items)
	assert.NoError(t, err)
	assert.Equal(t, []string{"item1", "item2", "item3"}, items)

	// Test integer array
	v, err = y.Get("numbers")
	assert.NoError(t, err)
	assert.NotNil(t, v)

	var numbers []int
	err = json.Unmarshal(v, &numbers)
	assert.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3}, numbers)
}

func TestYamlObjects(t *testing.T) {
	// Create a temporary YAML file with nested objects
	file, err := ioutil.TempFile("", "TestYamlObjects")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())
	yamlContent := `
database:
  host: localhost
  port: 5432
  name: mydb
server:
  address: 0.0.0.0
  ports:
    - 8080
    - 8443
`
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(yamlContent), 0))

	y, err := YamlLoader(file.Name()).Get()
	assert.NoError(t, err)

	// Test nested object
	v, err := y.Get("database")
	assert.NoError(t, err)
	assert.NotNil(t, v)

	var db map[string]interface{}
	err = json.Unmarshal(v, &db)
	assert.NoError(t, err)
	assert.Equal(t, "localhost", db["host"])
	assert.Equal(t, float64(5432), db["port"]) // JSON numbers are float64
	assert.Equal(t, "mydb", db["name"])

	// Test object with nested array
	v, err = y.Get("server")
	assert.NoError(t, err)
	assert.NotNil(t, v)

	var server map[string]interface{}
	err = json.Unmarshal(v, &server)
	assert.NoError(t, err)
	assert.Equal(t, "0.0.0.0", server["address"])

	ports := server["ports"].([]interface{})
	assert.Equal(t, float64(8080), ports[0])
	assert.Equal(t, float64(8443), ports[1])
}

func TestYamlEmptyString(t *testing.T) {
	// Create a temporary YAML file with an empty string value
	file, err := ioutil.TempFile("", "TestYamlEmptyString")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())
	yamlContent := `
empty_value: ""
non_empty: "hello"
`
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(yamlContent), 0))

	y, err := YamlLoader(file.Name()).Get()
	assert.NoError(t, err)

	// Empty string should return nil
	v, err := y.Get("empty_value")
	assert.NoError(t, err)
	assert.Nil(t, v)

	// Non-empty string should return the value
	v, err = y.Get("non_empty")
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), v)
}

func TestDistconfGetArraysAndObjects(t *testing.T) {
	// Create a temporary YAML file with arrays and objects
	file, err := ioutil.TempFile("", "TestDistconfGetArraysAndObjects")
	assert.NoError(t, err)
	defer func() {
		log.IfErr(log.Panic, os.Remove(file.Name()))
	}()

	log.IfErr(log.Panic, file.Close())
	yamlContent := `
string_val: hello
items:
  - item1
  - item2
  - item3
database:
  host: localhost
  port: 5432
`
	assert.NoError(t, ioutil.WriteFile(file.Name(), []byte(yamlContent), 0))

	// Use FromLoaders to create Distconf (the typical usage pattern)
	backs := []BackingLoader{YamlLoader(file.Name())}
	conf := FromLoaders(backs)
	defer conf.Close()

	// Test that typed methods still work
	strVal := conf.Str("string_val", "default")
	assert.Equal(t, "hello", strVal.Get())

	// Test Get() for string value
	v, err := conf.Get("string_val")
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), v)

	// Test Get() for array
	v, err = conf.Get("items")
	assert.NoError(t, err)
	assert.NotNil(t, v)

	var items []string
	err = json.Unmarshal(v, &items)
	assert.NoError(t, err)
	assert.Equal(t, []string{"item1", "item2", "item3"}, items)

	// Test Get() for object
	v, err = conf.Get("database")
	assert.NoError(t, err)
	assert.NotNil(t, v)

	var db map[string]interface{}
	err = json.Unmarshal(v, &db)
	assert.NoError(t, err)
	assert.Equal(t, "localhost", db["host"])
	assert.Equal(t, float64(5432), db["port"])

	// Test Get() for non-existent key
	v, err = conf.Get("non_existent_key")
	assert.NoError(t, err)
	assert.Nil(t, v)
}

package distconf

import (
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

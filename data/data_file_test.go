package data

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	dataFile2, err := OpenDataFile(os.TempDir(), 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)

	dataFile3, err := OpenDataFile(os.TempDir(), 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile3)
}

func TestDataFile_Write(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	data := []byte("hello")
	err = dataFile.Write(data)
	assert.Nil(t, err)

	data = []byte("world")
	err = dataFile.Write(data)
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	data := []byte("hello")
	err = dataFile.Write(data)
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)
}

package m2mpdb

import (
	"bytes"
	"testing"
)

func TestFileWrite(t *testing.T) {
	NewSessionSimple("ks_test")

	file := NewRegistryFile(NewRegistryNode("/tmp/TestFileWrite/"))
	file.SetBlockSize(5)

	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}

	{ // We write it
		writer := file.Writer()
		if l, err := writer.Write(data); l != len(data) || err != nil {
			t.Fatalf("Error writing, written = %d, err = %v", l, err)
		}
	}

	{ // We check the data blocks
		if block, err := file.Block(0); err != nil || bytes.Compare(block, data[0:5]) != 0 {
			t.Fatalf("Bad block: %v, err: %v", block, err)
		}
		if block, err := file.Block(1); err != nil || bytes.Compare(block, data[5:7]) != 0 {
			t.Fatalf("Bad block: %v, err: %v", block, err)
		}
	}

	if file.Size() != len(data) { // We check the size
		t.Fatalf("Bad size: ", file.Size())
	}

}

func TestFileRewrite(t *testing.T) {
	NewSessionSimple("ks_test")

	file := NewRegistryFile(NewRegistryNode("/tmp/TestFileReWrite/"))
	file.SetBlockSize(5)

	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}

	{
		writer := file.Writer()
		writer.Write(data)
	}

	{ // We check the data blocks
		if block, err := file.Block(0); err != nil || bytes.Compare(block, data[0:5]) != 0 {
			t.Fatalf("Bad block: %v, err: %v", block, err)
		}
		if block, err := file.Block(1); err != nil || bytes.Compare(block, data[5:7]) != 0 {
			t.Fatalf("Bad block: %v, err: %v", block, err)
		}
	}
}

func TestFileTruncate(t *testing.T) {
	NewSessionSimple("ks_test")

	file := NewRegistryFile(NewRegistryNode("/tmp/TestFileTruncate/"))
	file.SetBlockSize(5)

	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}

	{
		writer := file.Writer()
		writer.Write(data)
	}

	file.Truncate(6)

	{ // We check the data blocks
		if block, err := file.Block(0); err != nil || bytes.Compare(block, data[0:5]) != 0 {
			t.Fatalf("Bad block: %v, err: %v", block, err)
		}
		if block, err := file.Block(1); err != nil || bytes.Compare(block, data[5:6]) != 0 {
			t.Fatalf("Bad block: %v / %v, err: %v", block, data[5:6], err)
		}
	}
}

func TestFileSeek(t *testing.T) {
	NewSessionSimple("ks_test")

	file := NewRegistryFile(NewRegistryNode("/tmp/TestFileSeek/"))
	file.SetBlockSize(5)

	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}

	{
		writer := file.Writer()
		writer.Write(data)
	}

	data2 := make([]byte, len(data))
	copy(data2, data)
	data2 = append(data2, []byte{0x09, 0x0A, 0x0B, 0x0C, 0x0D}...)

	{
		writer := file.Writer()
		writer.Seek(len(data), 0)
		writer.Write(data2[len(data):])
	}

	{ // We check the data blocks
		if block, err := file.Block(0); err != nil || bytes.Compare(block, data2[0:5]) != 0 {
			t.Fatalf("Bad block: %v / %v, err: %v", block, data2[0:5], err)
		}
		if block, err := file.Block(1); err != nil || bytes.Compare(block, data2[5:10]) != 0 {
			t.Fatalf("Bad block: %v / %v, err: %v", block, data[5:6], err)
		}
		if block, err := file.Block(2); err != nil || bytes.Compare(block, data2[10:13]) != 0 {
			t.Fatalf("Bad block: %v / %v, err: %v", block, data[10:13], err)
		}
	}
}

func TestFileRead(t *testing.T) {
	NewSessionSimple("ks_test")

	file := NewRegistryFile(NewRegistryNode("/tmp/TestFileRead/"))
	file.SetBlockSize(5)

	writeData := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}

	{ // We prepare some data
		writer := file.Writer()

		if l, err := writer.Write(writeData); l != len(writeData) || err != nil {
			t.Fatalf("Error writing, written = %d, err = %v", l, err)
		}
	}

	{ // We try to read it in two passes

		readData := make([]byte, len(writeData))

		{
			reader := file.Reader()
			if l, err := reader.Read(readData[0:5]); err != nil || l != 5 {
				t.Fatalf("Bad reading: err=%v, l=%d", err, l)
			}
			if l, err := reader.Read(readData[5:7]); err != nil || l != 2 {
				t.Fatalf("Bad reading: err=%v, l=%d", err, l)
			}
			if _, err := reader.Read(readData); err == nil || err.Error() != "EOF" {
				t.Fatalf("Invalid EOF: %v", err)
			}
		}

		if bytes.Compare(writeData, readData) != 0 {
			t.Fatalf("Different data read: %v != %v", writeData, readData)
		}
	}
}

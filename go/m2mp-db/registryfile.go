package m2mpdb

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

const (
	prop_name           = "fname"
	prop_type           = "ftype"
	prop_size           = "fsize"
	prop_ok             = "getOk"
	prop_block_size     = "blsize"
	table_registry_data = "RegistryNodeData"
	default_block_size  = 512 * 1024 // 512 KB
)

type RegistryFile struct {
	Node *RegistryNode
	path string
}

func NewRegistryFile(node *RegistryNode) *RegistryFile {
	return &RegistryFile{Node: node, path: node.Path}
}

func (this *RegistryFile) Create() {
	this.Node.Create()
	this.Node.SetValue(".is_file", "1")
}

func (this *RegistryFile) Exists() bool {
	return this.Node.Exists() && this.Node.Value(".is_file") == "1"
}

func (this *RegistryFile) SetType(mime string) error {
	return this.Node.SetValue(prop_type, mime)
}

func (this *RegistryFile) Type() string {
	return this.Node.Value(prop_type)
}

func (this *RegistryFile) SetName(name string) error {
	return this.Node.SetValue(prop_name, name)
}

func (this *RegistryFile) Name() string {
	return this.Node.Value(prop_name)
}

func (this *RegistryFile) SetSize(size int) error {
	return this.Node.SetValue(prop_size, fmt.Sprintf("%d", size))
}

func (this *RegistryFile) Size() int {
	if size, err := strconv.Atoi(this.Node.Value(prop_size)); err == nil {
		return size
	} else {
		return -1
	}
}

func (this *RegistryFile) SetBlockSize(size int) error {
	return this.Node.SetValue(prop_block_size, fmt.Sprintf("%d", size))
}

func (this *RegistryFile) BlockSize() int {
	if size, err := strconv.Atoi(this.Node.Value(prop_block_size)); err == nil {
		return size
	} else {
		this.Node.SetValue(prop_block_size, fmt.Sprintf("%d", default_block_size))
		return default_block_size
	}
}

func (this *RegistryFile) SetBlock(blockNb int, data []byte) error {
	return shared.session.Query("INSERT INTO "+table_registry_data+" ( path, block, data ) VALUES ( ?, ?, ? );", this.path, blockNb, data).Exec()
}

func (this *RegistryFile) DelBlock(blockNb int) error {
	return shared.session.Query("DELETE FROM "+table_registry_data+" WHERE path = ? AND blockNb = ?;", this.path, blockNb).Exec()
}

func (this *RegistryFile) Block(blockNb int) ([]byte, error) {
	var data []byte
	err := shared.session.Query("SELECT data FROM "+table_registry_data+" WHERE path = ? AND block = ?;", this.path, blockNb).Scan(&data)
	return data, err
}

func (this *RegistryFile) SetOk(ok bool) {
	var value string
	if ok {
		value = "1"
	} else {
		value = "0"
	}
	this.Node.SetValue(prop_ok, value)
}

func (this *RegistryFile) Ok() bool {
	return this.Node.Value(prop_ok) == "1"
}

const (
	mode_read  = os.O_RDONLY
	mode_write = os.O_WRONLY
)

type RegistryFilePointer struct {
	file      *RegistryFile
	buffer    []byte
	blockSize int
	offset    int
	size      int
	mode      int
}

func (this *RegistryFile) Writer() *RegistryFilePointer {
	this.Create()
	return &RegistryFilePointer{file: this, blockSize: this.BlockSize(), mode: mode_write}
}

func (this *RegistryFile) WriterEdit() *RegistryFilePointer {
	this.Create()
	return &RegistryFilePointer{file: this, blockSize: this.BlockSize(), size: this.Size(), mode: mode_write}
}

func (this *RegistryFile) Reader() *RegistryFilePointer {
	return &RegistryFilePointer{file: this, blockSize: this.BlockSize(), size: this.Size(), mode: mode_read}
}

func (this *RegistryFile) Truncate(size int) error {

	fileSize := this.Size()
	blockSize := this.BlockSize()

	// We cannot truncate to more than the file size
	if size > fileSize {
		size = fileSize
	}

	offset := size
	for offset < fileSize {
		// Current block
		currentBlock := offset / blockSize

		// Current block offset
		blockOffset := offset - (currentBlock * blockSize)

		if blockOffset == 0 {
			this.DelBlock(currentBlock)
		} else {
			// This part isn't mandatory. It's to avoid confusion with useless data.
			if data, err := this.Block(currentBlock); err == nil {
				if err := this.SetBlock(currentBlock, data[0:blockOffset]); err != nil {
					return err
				}
			} else {
				return err
			}
		}

		// We increment the offset
		offset += blockSize
	}

	// In the end we save this as the final size
	this.SetSize(size)
	return nil
}

func (this *RegistryFile) Delete(forReal bool) {
	if forReal {
		this.Truncate(0)
	}

	this.Node.Delete(forReal)
}

const debug_registryfile = false

// Writer interface
func (this *RegistryFilePointer) Write(data []byte) (int, error) {
	dataSize := len(data)
	inputOffset := 0

	var err error = nil

	if debug_registryfile {
		log.Printf("Write( [%d]byte );", len(data))
	}

	// We will write as much blocks as needed
	for inputOffset < dataSize {
		// Current block
		currentBlock := this.offset / this.blockSize

		// Current block offset
		blockOffset := this.offset - (currentBlock * this.blockSize)

		// The size of what we can copy in this block
		copySize := this.blockSize - blockOffset

		// If we don't have that much data remaining, we reduce the copySize
		if remaining := dataSize - inputOffset; copySize > remaining {
			copySize = remaining
		}

		if debug_registryfile {
			log.Printf("Write: currentBlock=%d, blockOffset=%d, copySize=%d, offset=%d, blockSize=%d", currentBlock, blockOffset, copySize, this.offset, this.blockSize)
		}

		// If we are at the offset zero of the block or
		// if existing buffer doesn't exist, we read it.
		if blockOffset == 0 || this.buffer == nil { // "this.buffer == nil" will only be used when we add Skip(n int)
			// We try to load any existing data in the buffer (this will be used to allow to skip data on an existing file)
			if this.buffer, err = this.file.Block(currentBlock); err != nil && err.Error() != "not found" {
				break
			} else if len(this.buffer) != this.blockSize {
				// We might need to create a bigger buffer as the last block might not
				// reach its full size
				newBuffer := make([]byte, this.blockSize)
				copy(newBuffer, this.buffer)
				this.buffer = newBuffer
			}
		}

		// We copy the amount of data we need to copy
		copy(this.buffer[blockOffset:], data[inputOffset:inputOffset+copySize])

		// We write the block
		if err = this.file.SetBlock(currentBlock, this.buffer[0:blockOffset+copySize]); err != nil {
			// And stop if we failed
			break
		}

		// We increment the inputOffset and the global offset
		inputOffset += copySize
		this.offset += copySize

		// We write the file size only if the offset is superior to the current file size
		if this.offset > this.size {
			if err = this.file.SetSize(this.offset); err != nil {
				break
			}
			this.size = this.offset
		}
	}
	return inputOffset, err
}

func (this *RegistryFilePointer) Seek(offset int, whence int) {
	previousBlockNb := this.offset / this.blockSize

	switch whence {
	case 0:
		this.offset = offset
	case 1:
		this.offset += offset
	case 2:
		this.offset = this.size - offset
	}

	if this.offset < 0 {
		this.offset = 0
	} else if this.mode == mode_read && this.offset > this.size {
		this.offset = this.size
	}

	blockNb := this.offset / this.blockSize

	if previousBlockNb != blockNb {
		this.buffer = nil
	}

}

// Reader interface
func (this *RegistryFilePointer) Read(data []byte) (int, error) {
	dataSize := len(data)

	// We might not have enough data to fill the buffer
	if remaining := this.size - this.offset; dataSize > remaining {
		dataSize = remaining

		// If we don't have anything sent, it's an EOF
		if dataSize == 0 {
			return 0, io.EOF
		}
	}

	outputOffset := 0

	var err error = nil

	for outputOffset < dataSize {
		// Current block
		currentBlock := this.offset / this.blockSize

		// Current block offset
		blockOffset := this.offset - (currentBlock * this.blockSize)

		// Data to copy from the block
		copySize := this.blockSize - blockOffset

		// We might not have that much data available in our target buffer
		if remaining := dataSize - outputOffset; copySize > remaining {
			copySize = remaining
		}

		// If we are at the offset zero of the block or
		// if existing buffer doesn't exist, we read it.
		if blockOffset == 0 || this.buffer == nil { // "this.buffer == nil" will only be used when we add Skip(n int)
			// We try to load any existing data in the buffer (this will be used to allow to skip data on an existing file)
			if this.buffer, err = this.file.Block(currentBlock); err != nil {
				break
			}
		}

		// We copy the amount of data we need to copy
		copy(data[outputOffset:], this.buffer[blockOffset:blockOffset+copySize])

		// We increment the inputOffset and the global offset
		outputOffset += copySize
		this.offset += copySize
	}

	return outputOffset, err
}

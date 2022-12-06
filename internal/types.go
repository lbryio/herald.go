package internal

// internal types that need their own file to avoid circular imports.

// HeightHash struct for the height subscription endpoint.
type HeightHash struct {
	Height      uint64
	BlockHash   []byte
	BlockHeader []byte
}

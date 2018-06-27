package dataunit

// Size represents
type Size int64

func (s Size) convert(scale Size) float64 {
	return float64(s/scale) + float64(s%scale)/float64(scale)
}

// Bytes returns the dataunit as a uint64 of bytes
func (s Size) Bytes() int64 {
	return int64(s)
}

// Kilobytes returns the dataunit as a float64 of kilobytes
func (s Size) Kilobytes() float64 {
	return s.convert(Kilobyte)
}

// Megabytes returns the dataunit as a float64 of kilobytes
func (s Size) Megabytes() float64 {
	return s.convert(Megabyte)
}

// Gigabytes returns the dataunit as a float64 of gigabytes
func (s Size) Gigabytes() float64 {
	return s.convert(Gigabyte)
}

// Terabytes returns the dataunit as a float64 of terabytes
func (s Size) Terabytes() float64 {
	return s.convert(Terabyte)
}

// Petabytes returns the dataunit as a float64 of petabytes
func (s Size) Petabytes() float64 {
	return s.convert(Petabyte)
}

// Exabytes returns the dataunit as a float64 of exabytes
func (s Size) Exabytes() float64 {
	return s.convert(Exabyte)
}

// Common data dataunits
const (
	Byte     Size = 1
	Kilobyte      = 1024 * Byte
	Megabyte      = 1024 * Kilobyte
	Gigabyte      = 1024 * Megabyte
	Terabyte      = 1024 * Gigabyte
	Petabyte      = 1024 * Terabyte
	Exabyte       = 1024 * Petabyte
)

package dataunit

// USize represents
type USize uint64

func (s USize) convert(scale USize) float64 {
	return float64(s/scale) + float64(s%scale)/float64(scale)
}

// Bytes returns the udataunit as a uint64 of bytes
func (s USize) Bytes() uint64 {
	return uint64(s)
}

// Kilobytes returns the udataunit as a float64 of kilobytes
func (s USize) Kilobytes() float64 {
	return s.convert(UKilobyte)
}

// Megabytes returns the udataunit as a float64 of kilobytes
func (s USize) Megabytes() float64 {
	return s.convert(UMegabyte)
}

// Gigabytes returns the udataunit as a float64 of gigabytes
func (s USize) Gigabytes() float64 {
	return s.convert(UGigabyte)
}

// Terabytes returns the udataunit as a float64 of terabytes
func (s USize) Terabytes() float64 {
	return s.convert(UTerabyte)
}

// Petabytes returns the udataunit as a float64 of petabytes
func (s USize) Petabytes() float64 {
	return s.convert(UPetabyte)
}

// Exabytes returns the udataunit as a float64 of exabytes
func (s USize) Exabytes() float64 {
	return s.convert(UExabyte)
}

// Common data sizes stored as unsigned integers
const (
	UByte     USize = 1
	UKilobyte       = 1024 * UByte
	UMegabyte       = 1024 * UKilobyte
	UGigabyte       = 1024 * UMegabyte
	UTerabyte       = 1024 * UGigabyte
	UPetabyte       = 1024 * UTerabyte
	UExabyte        = 1024 * UPetabyte
)

package dataunit

import "testing"

func Test_SizeBytes(t *testing.T) {
	tests := []struct {
		name string
		s    Size
		u    USize
		want int64
	}{
		{
			name: "test bytes",
			s:    1 * Byte,
			u:    1 * UByte,
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.Bytes(); got != int64(tt.want) {
				t.Errorf("Size.Bytes() = %v, want %v", got, tt.want)
			}
			if got := tt.u.Bytes(); got != uint64(tt.want) {
				t.Errorf("USize.Bytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSize_Kilobytes(t *testing.T) {
	tests := []struct {
		name string
		s    Size
		u    USize
		want float64
	}{
		{
			name: "test kilobytes",
			s:    1536 * Byte,
			u:    1536 * UByte,
			want: float64(1.5),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.Kilobytes(); got != tt.want {
				t.Errorf("Size.Kilobytes() = %v, want %v", got, tt.want)
			}
			if got := tt.u.Kilobytes(); got != tt.want {
				t.Errorf("USize.Kilobytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSize_Megabytes(t *testing.T) {
	tests := []struct {
		name string
		s    Size
		u    USize
		want float64
	}{
		{
			name: "test megabytes",
			s:    1536 * Kilobyte,
			u:    1536 * UKilobyte,
			want: float64(1.5),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.Megabytes(); got != tt.want {
				t.Errorf("Size.Megabytes() = %v, want %v", got, tt.want)
			}
			if got := tt.u.Megabytes(); got != tt.want {
				t.Errorf("USize.Megabytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSize_Gigabytes(t *testing.T) {
	tests := []struct {
		name string
		s    Size
		u    USize
		want float64
	}{
		{
			name: "test gigabytes",
			s:    1536 * Megabyte,
			u:    1536 * UMegabyte,
			want: float64(1.5),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.Gigabytes(); got != tt.want {
				t.Errorf("Size.Gigabytes() = %v, want %v", got, tt.want)
			}
			if got := tt.u.Gigabytes(); got != tt.want {
				t.Errorf("USize.Gigabytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSize_Terabytes(t *testing.T) {
	tests := []struct {
		name string
		s    Size
		u    USize
		want float64
	}{
		{
			name: "test terabytes",
			s:    1536 * Gigabyte,
			u:    1536 * UGigabyte,
			want: float64(1.5),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.Terabytes(); got != tt.want {
				t.Errorf("Size.Terabytes() = %v, want %v", got, tt.want)
			}
			if got := tt.u.Terabytes(); got != tt.want {
				t.Errorf("USize.Terabytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSize_Petabytes(t *testing.T) {
	tests := []struct {
		name string
		s    Size
		u    USize
		want float64
	}{
		{
			name: "test petabytes",
			s:    1536 * Terabyte,
			u:    1536 * UTerabyte,
			want: float64(1.5),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.Petabytes(); got != tt.want {
				t.Errorf("Size.Petabytes() = %v, want %v", got, tt.want)
			}
			if got := tt.u.Petabytes(); got != tt.want {
				t.Errorf("USize.Petabytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSize_Exabytes(t *testing.T) {
	tests := []struct {
		name string
		s    Size
		u    USize
		want float64
	}{
		{
			name: "test exabytes",
			s:    1536 * Petabyte,
			u:    1536 * UPetabyte,
			want: float64(1.5),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.Exabytes(); got != tt.want {
				t.Errorf("Size.Exabytes() = %v, want %v", got, tt.want)
			}
			if got := tt.u.Exabytes(); got != tt.want {
				t.Errorf("USize.Exabytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

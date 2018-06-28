package dataunit

import "testing"

func TestUSize_Bytes(t *testing.T) {
	tests := []struct {
		name string
		s    USize
		want uint64
	}{
		{
			name: "test ubytes",
			s:    1 * UByte,
			want: uint64(1),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.Bytes(); got != tt.want {
				t.Errorf("USize.Bytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUSize_Kilobytes(t *testing.T) {
	tests := []struct {
		name string
		s    USize
		want float64
	}{
		{
			name: "test ukilobytes",
			s:    1536 * UByte,
			want: float64(1.5),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.Kilobytes(); got != tt.want {
				t.Errorf("USize.Kilobytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUSize_Megabytes(t *testing.T) {
	tests := []struct {
		name string
		s    USize
		want float64
	}{
		{
			name: "test umegabytes",
			s:    1536 * UKilobyte,
			want: float64(1.5),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.Megabytes(); got != tt.want {
				t.Errorf("USize.Megabytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUSize_Gigabytes(t *testing.T) {
	tests := []struct {
		name string
		s    USize
		want float64
	}{
		{
			name: "test ugigabytes",
			s:    1536 * UMegabyte,
			want: float64(1.5),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.Gigabytes(); got != tt.want {
				t.Errorf("USize.Gigabytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUSize_Terabytes(t *testing.T) {
	tests := []struct {
		name string
		s    USize
		want float64
	}{
		{
			name: "test uterabytes",
			s:    1536 * UGigabyte,
			want: float64(1.5),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.Terabytes(); got != tt.want {
				t.Errorf("USize.Terabytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUSize_Petabytes(t *testing.T) {
	tests := []struct {
		name string
		s    USize
		want float64
	}{
		{
			name: "test upetabytes",
			s:    1536 * UTerabyte,
			want: float64(1.5),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.Petabytes(); got != tt.want {
				t.Errorf("USize.Petabytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUSize_Exabytes(t *testing.T) {
	tests := []struct {
		name string
		s    USize
		want float64
	}{
		{
			name: "test uexabytes",
			s:    1536 * UPetabyte,
			want: float64(1.5),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.Exabytes(); got != tt.want {
				t.Errorf("USize.Exabytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

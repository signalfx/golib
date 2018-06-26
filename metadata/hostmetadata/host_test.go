package hostmetadata

import (
	"errors"
	"reflect"
	"testing"

	"github.com/shirou/gopsutil/mem"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/host"
)

func TestGetCPUInfo(t *testing.T) {
	type testfixture struct {
		cpuInfo   func() ([]cpu.InfoStat, error)
		cpuCounts func(bool) (int, error)
	}
	tests := []struct {
		name     string
		fixtures testfixture
		wantInfo map[string]string
		wantErr  bool
	}{
		{
			name: "successful host cpu info",
			fixtures: testfixture{
				cpuInfo: func() ([]cpu.InfoStat, error) {
					return []cpu.InfoStat{
						{
							ModelName: "testmodelname",
							Cores:     4,
						},
						{
							ModelName: "testmodelname2",
							Cores:     4,
						},
					}, nil
				},
				cpuCounts: func(bool) (int, error) {
					return 2, nil
				},
			},
			wantInfo: map[string]string{
				"host_physical_cpus": "2",
				"host_cpu_cores":     "8",
				"host_cpu_model":     "testmodelname2",
				"host_logical_cpus":  "2",
			},
		},
		{
			name: "unsuccessful host cpu info (missing cpu info)",
			fixtures: testfixture{
				cpuInfo: func() ([]cpu.InfoStat, error) {
					return nil, errors.New("bad cpu info")
				},
			},
			wantErr: true,
		},
		{
			name: "unsuccessful host cpu info (missing cpu counts)",
			fixtures: testfixture{
				cpuInfo: func() ([]cpu.InfoStat, error) {
					return []cpu.InfoStat{
						{
							ModelName: "testmodelname",
							Cores:     4,
						},
						{
							ModelName: "testmodelname2",
							Cores:     4,
						},
					}, nil
				},
				cpuCounts: func(bool) (int, error) {
					return 0, errors.New("bad cpu counts")
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cpuInfo = tt.fixtures.cpuInfo
			cpuCounts = tt.fixtures.cpuCounts
			gotInfo, err := GetCPUInfo()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetCPUInfo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotInfo, tt.wantInfo) {
				t.Errorf("GetCPUInfo() = %v, want %v", gotInfo, tt.wantInfo)
			}
		})
	}
}

func TestGetKernelInfo(t *testing.T) {
	type testfixture struct {
		hostInfo func() (*host.InfoStat, error)
		hostEtc  string
	}
	tests := []struct {
		name         string
		testfixtures testfixture
		wantInfo     map[string]string
		wantErr      bool
	}{
		{
			name: "get kernel info",
			testfixtures: testfixture{
				hostInfo: func() (*host.InfoStat, error) {
					return &host.InfoStat{
						OS:              "linux",
						KernelVersion:   "4.4.0-112-generic",
						Platform:        "ubuntu",
						PlatformVersion: "16.04",
					}, nil
				},
				hostEtc: "./testdata/lsb-release",
			},
			wantInfo: map[string]string{
				"host_kernel_name":    "linux",
				"host_kernel_version": "4.4.0-112-generic",
				"host_os_name":        "ubuntu",
				"host_os_version":     "16.04",
				"host_linux_version":  "Ubuntu 18.04 LTS",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hostInfo = tt.testfixtures.hostInfo
			HostETC = tt.testfixtures.hostEtc
			gotInfo, err := GetKernelInfo()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetKernelInfo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotInfo, tt.wantInfo) {
				t.Errorf("GetKernelInfo() = %v, want %v", gotInfo, tt.wantInfo)
			}
		})
	}
}

func Test_getLinuxVersion(t *testing.T) {
	tests := []struct {
		name    string
		etc     string
		want    string
		wantErr bool
	}{
		{
			name: "lsb-release",
			etc:  "./testdata/lsb-release",
			want: "Ubuntu 18.04 LTS",
		},
		{
			name: "os-release",
			etc:  "./testdata/os-release",
			want: "Debian GNU/Linux 9 (stretch)",
		},
		{
			name: "centos-release",
			etc:  "./testdata/centos-release",
			want: "CentOS Linux release 7.5.1804 (Core)",
		},
		{
			name: "redhat-release",
			etc:  "./testdata/redhat-release",
			want: "Red Hat Enterprise Linux Server release 7.5 (Maipo)",
		},
		{
			name: "system-release",
			etc:  "./testdata/system-release",
			want: "CentOS Linux release 7.5.1804 (Core)",
		},
		{
			name:    "no release returns error",
			etc:     "./testdata",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			HostETC = tt.etc
			got, err := getLinuxVersion()
			if (err != nil) != tt.wantErr {
				t.Errorf("getLinuxVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getLinuxVersion() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetMemory(t *testing.T) {
	tests := []struct {
		name             string
		memVirtualMemory func() (*mem.VirtualMemoryStat, error)
		want             map[string]string
		wantErr          bool
	}{
		{
			name: "memory utilization",
			memVirtualMemory: func() (*mem.VirtualMemoryStat, error) {
				return &mem.VirtualMemoryStat{
					Total: 1024,
				}, nil
			},
			want: map[string]string{"host_mem_total": "1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memVirtualMemory = tt.memVirtualMemory
			got, err := GetMemory()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetMemory() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetMemory() = %v, want %v", got, tt.want)
			}
		})
	}
}

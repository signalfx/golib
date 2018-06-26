package hostmetadata

import (
	"errors"
	"fmt"
	"io/ioutil"
	"regexp"
	"strconv"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/mem"
)

// HostETC is the path to host etc
var HostETC = "/etc"

// Map library functions to unexported package variables for testing purposes.
// It would be great if we could patch this somehow
var cpuInfo = cpu.Info
var cpuCounts = cpu.Counts
var memVirtualMemory = mem.VirtualMemory
var hostInfo = host.Info

// GetCPUInfo - adds information about the host cpu to the supplied map
func GetCPUInfo() (info map[string]string, err error) {
	var cpus []cpu.InfoStat
	if cpus, err = cpuInfo(); err != nil {
		return info, err
	}

	var logicalCPU int
	if logicalCPU, err = cpuCounts(true); err != nil {
		return info, err
	}

	var CPUModel string
	var numCores int64
	for _, cpu := range cpus {
		numCores = int64(cpu.Cores) + numCores
		// TODO: This is not ideal... if there are different processors
		// we will only report one of the models... This is unlikely to happen,
		// but it could
		CPUModel = cpu.ModelName
	}

	info = map[string]string{
		"host_physical_cpus": strconv.Itoa(len(cpus)),
		"host_cpu_cores":     strconv.FormatInt(numCores, 10),
		"host_cpu_model":     CPUModel,
		"host_logical_cpus":  strconv.Itoa(logicalCPU),
	}

	return info, err
}

// GetKernelInfo - adds information about the host kernel to the supplied map
func GetKernelInfo() (info map[string]string, err error) {
	info = make(map[string]string, 5)
	hInfo, err := hostInfo()
	if err == nil {
		info["host_kernel_name"] = hInfo.OS
		info["host_kernel_version"] = hInfo.KernelVersion
		info["host_os_name"] = hInfo.Platform
		info["host_os_version"] = hInfo.PlatformVersion

		if hInfo.OS == "linux" {
			if value, errr := getLinuxVersion(); errr == nil {
				info["host_linux_version"] = value
			}
		}
	}

	return info, err
}

// getLinuxVersion - adds information about the host linux version to the supplied map
func getLinuxVersion() (string, error) {
	if value, er := getStringFromFile(`DISTRIB_DESCRIPTION="(.*)"`, fmt.Sprintf("%s/lsb-release", HostETC)); er == nil {
		return value, nil
	}
	if value, er := getStringFromFile(`PRETTY_NAME="(.*)"`, fmt.Sprintf("%s/os-release", HostETC)); er == nil {
		return value, nil
	}
	if value, er := ioutil.ReadFile(fmt.Sprintf("%s/centos-release", HostETC)); er == nil {
		return string(value), nil
	}
	if value, er := ioutil.ReadFile(fmt.Sprintf("%s/redhat-release", HostETC)); er == nil {
		return string(value), nil
	}
	if value, er := ioutil.ReadFile(fmt.Sprintf("%s/system-release", HostETC)); er == nil {
		return string(value), nil
	}
	return "", errors.New("unable to find linux version")
}

// GetMemory adds information about the host memory to the supplied map
func GetMemory() (map[string]string, error) {
	info := make(map[string]string, 1)
	mem, err := memVirtualMemory()
	if err == nil {
		info["host_mem_total"] = strconv.FormatUint(mem.Total/1024, 10)
	}
	return info, err
}

func getStringFromFile(pattern string, path string) (string, error) {
	var err error
	var file []byte
	var reg = regexp.MustCompile(pattern)
	if file, err = ioutil.ReadFile(path); err == nil {
		if match := reg.FindSubmatch(file); len(match) > 1 {
			return string(match[1]), nil
		}
	}
	return "", err
}

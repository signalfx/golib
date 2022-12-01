//go:build !linux || ppc || ppc64 || ppc64le
// +build !linux ppc ppc64 ppc64le

package hostmetadata

func fillPlatformSpecificOSData(info *OS) error {
	return nil
}

func fillPlatformSpecificCPUData(info *CPU) error {
	return nil
}

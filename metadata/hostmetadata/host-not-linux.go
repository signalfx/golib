//go:build !linux || ppc || ppc64 || ppc64le
// +build !linux ppc ppc64 ppc64le

package hostmetadata

func fillPlatformSpecificOSData(_ *OS) error {
	return nil
}

func fillPlatformSpecificCPUData(_ *CPU) error {
	return nil
}

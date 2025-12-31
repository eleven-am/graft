//go:build !unix

package bootstrap

func getDiskUsageForPath(path string) float64 {
	return 0.0
}

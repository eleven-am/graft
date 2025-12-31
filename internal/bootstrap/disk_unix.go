//go:build unix

package bootstrap

import "syscall"

func getDiskUsageForPath(path string) float64 {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0.0
	}

	if stat.Blocks == 0 {
		return 0.0
	}

	usedBlocks := stat.Blocks - stat.Bfree
	return float64(usedBlocks) / float64(stat.Blocks)
}

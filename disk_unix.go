//go:build !windows

package main

import "golang.org/x/sys/unix"

func diskFree(path string) (int64, error) {
	var s unix.Statfs_t
	err := unix.Statfs(path, &s)
	return int64(s.Bavail) * int64(s.Bsize), err
}

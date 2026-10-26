//go:build windows

package main

import (
	"syscall"
	"unsafe"
)

func diskFree(path string) (int64, error) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var available uint64
	r, _, e := syscall.NewLazyDLL("kernel32.dll").NewProc("GetDiskFreeSpaceExW").Call(uintptr(unsafe.Pointer(p)), uintptr(unsafe.Pointer(&available)), 0, 0)
	if r == 0 {
		return 0, e
	}
	return int64(available), nil
}
